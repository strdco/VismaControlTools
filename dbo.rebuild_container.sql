/*

Copyright Daniel Hutmacher under Creative Commons 4.0 license with attribution.
http://creativecommons.org/licenses/by/4.0/

DISCLAIMER: This script may not be suitable to run in a production
            environment. I cannot assume any responsibility regarding
            the accuracy of the output information, performance
            impacts on your server, or any other consequence. If
            your juristiction does not allow for this kind of
            waiver/disclaimer, or if you do not accept these terms,
            you are NOT allowed to store, distribute or use this
            code in any way.

USAGE:      EXECUTE dbo.rebuild_container;

NOTE:       This procedure does not recalculate budget entries, only
            actuals (types 0, 1, 9 and 10).

*/

CREATE OR ALTER PROCEDURE dbo.rebuild_container
AS

DECLARE @top_year smallint=(SELECT MAX(yearno) FROM dbo.[year]);

BEGIN TRANSACTION;

    ALTER TABLE dbo.containerx DISABLE TRIGGER ALL;

    --- 9/10: these are actuals. type=9 means actuals for regular voucher series, 10 are actuals for simulation series:
    WITH actuals AS (
        SELECT r.[year], r.[period], CAST((CASE WHEN S.dmy IN (2, 4) THEN 10 ELSE 9 END) AS smallint) AS [type], r.cid,
               (CASE WHEN s.dmy IN (2, 4) THEN r.serie END) AS serie,
               r.account, r.currency, r.o1, r.o2, r.o3, r.o4, r.o5, r.o6, r.o7, r.o8, r.basecurrency,
               SUM(r.amount) AS amount,
               SUM(r.foramount) AS foramount,
               SUM(r.number) AS number,
               SUM(r.baseamount) AS baseamount
        FROM dbo.vourowx AS r WITH (TABLOCKX, HOLDLOCK)
        INNER JOIN dbo.seriex AS s ON r.[year]=s.[year] AND r.serie=s.serie
        WHERE r.altered!=2
          AND s.dmy!=3
        GROUP BY r.[year], r.[period], r.cid, r.serie, s.dmy,
                 r.account, r.currency, r.o1, r.o2, r.o3, r.o4, r.o5, r.o6, r.o7, r.o8, r.basecurrency),

    --- 0/1: recursive CTE to calculate the affected opening balances. Both 0 and 1 are opening balances,
    ---      but 1 cannot include the "result" account.
    opening AS (
        SELECT [year], [period], [type], cid, serie,
               account, CAST(currency AS varchar(3)) AS currency, o1, o2, o3, o4, o5, o6, o7, o8,
               basecurrency, amount, foramount, number, baseamount
        FROM actuals

        UNION ALL

        SELECT CAST(o.[year]+1 AS smallint), CAST(0 AS tinyint) AS [period], x.[type], o.cid, NULL AS serie,
               x.account, x.currency, x.o1, x.o2, x.o3, x.o4, x.o5, x.o6, x.o7, x.o8,
               o.basecurrency, o.amount, o.foramount, o.number, o.baseamount
        FROM opening AS o
        INNER JOIN dbo.accountx AS a ON o.account=a.account
        INNER JOIN dbo.accountx AS pl ON pl.[standard]=9 --- Profit/loss for the financial year (BAS: 2099)
        CROSS APPLY (
            --- Opening balances:
            SELECT CAST(1 AS smallint) AS [type],
                   o.account,
                   o.currency,
                   o.o1, o.o2, o.o3, o.o4,
                   o.o5, o.o6, o.o7, o.o8
            WHERE o.[type] IN (1, 9)

            UNION ALL

            --- Opening balances (with previous year's P&L in the @resaccount account):
            SELECT CAST(1 AS smallint) AS [type],
                   pl.account,
                   NULL AS currency,
                   NULL AS o1, NULL AS o2, NULL AS o3, NULL AS o4,
                   NULL AS o5, NULL AS o6, NULL AS o7, NULL AS o8
            WHERE o.[type]=9 AND a.acctype IN (3, 4)

            UNION ALL

            --- Previous year's actuals:
            SELECT CAST(0 AS smallint) AS [type],
                   o.account,
                   o.currency,
                   o.o1, o.o2, o.o3, o.o4,
                   o.o5, o.o6, o.o7, o.o8
            WHERE o.[type]=9
            ) AS x
        WHERE o.[year]<@top_year)

    SELECT cid, [year], [period], [type], account, currency, o1, o2, o3, o4, o5, o6, o7, o8,
            SUM(amount) AS amount, SUM(foramount) AS foramount, SUM(number) AS number,
            basecurrency, SUM(baseamount) AS baseamount, serie
    INTO #bal
    FROM opening
    GROUP BY [year], [period], [type], cid, serie, account, currency, o1, o2, o3, o4, o5, o6, o7, o8, basecurrency


    CREATE UNIQUE CLUSTERED INDEX UCIX ON #bal ([year], [period], [type], cid, serie, account, currency, o1, o2, o3, o4, o5, o6, o7, o8, basecurrency);


    WITH c AS (
        SELECT *
        FROM dbo.containerx
        WHERE [type] IN (0, 1, 9, 10))

    --- Because it only contains a WHEN MATCHED predicate, this merge is essentially just a glorified UPDATE.
    MERGE INTO c
    USING #bal AS bal ON EXISTS (
        SELECT   c.[year],   c.[period],   c.[type],   c.cid,   c.serie,   c.account,   c.currency,   c.o1,   c.o2,   c.o3,   c.o4,   c.o5,   c.o6,   c.o7,   c.o8,   c.basecurrency
        INTERSECT
        SELECT bal.[year], bal.[period], bal.[type], bal.cid, bal.serie, bal.account, bal.currency, bal.o1, bal.o2, bal.o3, bal.o4, bal.o5, bal.o6, bal.o7, bal.o8, bal.basecurrency)

    --- This balance shouldn't exist: delete it.
    WHEN NOT MATCHED BY SOURCE THEN DELETE

    --- If the matched balance comes to 0, delete the row entirely.
    WHEN MATCHED AND bal.amount=0
                 AND bal.foramount=0
                 AND bal.number=0
                 AND bal.baseamount=0 THEN DELETE

    --- Otherwise, just update the balances:
    WHEN MATCHED THEN
        UPDATE SET c.amount=bal.amount,
                   c.foramount=bal.foramount,
                   c.number=bal.number,
                   c.baseamount=bal.baseamount

    --- Create new balances:
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (cid, [year], model, [period], [type], account, currency, o1, o2, o3, o4, o5, o6, o7, o8, amount, foramount, number, basecurrency, baseamount, serie, flag)
        VALUES (bal.cid, bal.[year], NULL, bal.[period], bal.[type], bal.account, bal.currency, bal.o1, bal.o2, bal.o3, bal.o4, bal.o5, bal.o6, bal.o7, bal.o8, bal.amount, bal.foramount, bal.number, bal.basecurrency, bal.baseamount, bal.serie, 0);

    ALTER TABLE dbo.containerx ENABLE TRIGGER ALL;

COMMIT TRANSACTION;

GO
