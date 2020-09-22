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

DISCLAIMER: Obviously, deleting accounting data from a production environment is a
            criminal offense in most juristictions. Use this script to prepare dev
            or test environments only.

USAGE:      EXECUTE dbo.delete_vouchers @from_date='2020-01-01', @to_date=NULL;

*/

CREATE OR ALTER PROCEDURE dbo.delete_vouchers
    @cid        varchar(8)=NULL,
    @serie      varchar(8)=NULL,
    @from_vouno int=NULL,
    @to_vouno   int=NULL,
    @from_date  date=NULL,
    @to_date    date=NULL
AS

CREATE TABLE #vouchers (
    [year]      smallint NOT NULL,
    serie       varchar(8) COLLATE database_default NOT NULL,
    vouno       int NOT NULL,
    PRIMARY KEY CLUSTERED ([year], serie, vouno)
);

DECLARE @top_year       smallint    =(SELECT MAX(yearno) FROM dbo.[period]);                --- Highest year number in dbo.period

BEGIN TRANSACTION;

    --- Implicitly also placing exclusive table locks on these tables:
    ALTER TABLE dbo.voucherx DISABLE TRIGGER ALL;
    ALTER TABLE dbo.vourowx DISABLE TRIGGER ALL;
    ALTER TABLE dbo.containerx DISABLE TRIGGER ALL;

    --- Identify the vouchers we're going to delete:
    INSERT INTO #vouchers ([year], serie, vouno)
    SELECT v.[year], v.serie, v.vouno
    FROM dbo.voucherx AS v
    WHERE (v.cid=@cid OR @cid IS NULL)
      AND (v.serie=@serie OR @serie IS NULL)
      AND (@from_vouno<=v.vouno OR @from_vouno IS NULL)
      AND (v.vouno<=@to_vouno OR @to_vouno IS NULL)
      AND (@from_date<=v.[date] OR @from_date IS NULL)
      AND (v.[date]<=@to_date OR @to_date IS NULL)
    OPTION (RECOMPILE);


    --- Update dbo.containerx (types 0, 1, 9, 10)
    ---------------------------------------------

    --- 9/10: these are actuals. 9 means actuals for regular voucher series, 10 are actuals for simulation series:
    WITH actuals AS (
        SELECT r.[year], r.[period], CAST((CASE WHEN S.dmy IN (2, 4) THEN 10 ELSE 9 END) AS smallint) AS [type], r.cid,
               (CASE WHEN s.dmy IN (2, 4) THEN r.serie END) AS serie,
               r.account, r.currency, r.o1, r.o2, r.o3, r.o4, r.o5, r.o6, r.o7, r.o8, r.basecurrency,
               SUM(r.amount) AS amount,
               SUM(r.foramount) AS foramount,
               SUM(r.number) AS number,
               SUM(r.baseamount) AS baseamount
        FROM #vouchers AS v
        INNER JOIN dbo.vourowx AS r ON v.[year]=r.[year] AND v.serie=r.serie AND v.vouno=r.vouno
        INNER JOIN dbo.seriex AS s ON v.[year]=s.[year] AND v.serie=s.serie
        GROUP BY r.[year], r.[period], r.cid, r.serie, s.dmy,
                 r.account, r.currency, r.o1, r.o2, r.o3, r.o4, r.o5, r.o6, r.o7, r.o8, r.basecurrency),

    --- 0/1: recursive CTE to calculate the affected opening balances. Both 0 and 1 are opening balances,
    ---      but 1 cannot include the "result" account.
    opening AS (
        SELECT [year], [period], [type], cid, serie,
               account, currency, o1, o2, o3, o4, o5, o6, o7, o8,
               basecurrency, amount, foramount, number, baseamount
        FROM actuals

        UNION ALL

        SELECT CAST(o.[year]+1 AS smallint), CAST(0 AS tinyint) AS [period], x.[type], o.cid, NULL AS serie,
               x.account, o.currency, o.o1, o.o2, o.o3, o.o4, o.o5, o.o6, o.o7, o.o8,
               o.basecurrency, o.amount, o.foramount, o.number, o.baseamount
        FROM opening AS o
        INNER JOIN dbo.accountx AS a ON o.account=a.account
        INNER JOIN dbo.accountx AS r ON r.[standard]=9 --- Profit/loss for the financial year (BAS: 2099)
        CROSS APPLY (
            --- Opening balances (with previous year's P&L in the @resaccount account):
            SELECT CAST(1 AS smallint) AS [type],
                   (CASE WHEN a.acctype IN (3, 4) AND o.[type]=9 THEN r.account
                         ELSE o.account END) AS account
            WHERE o.[type] IN (1, 9)

            UNION ALL

            --- Previous year's actuals:
            SELECT CAST(0 AS smallint) AS [type],
                   o.account
            WHERE o.[type]=9
            ) AS x
        WHERE o.[year]<@top_year),

    bal AS (
        SELECT [year], [period], [type], cid, serie, account, currency, o1, o2, o3, o4, o5, o6, o7, o8, basecurrency,
               SUM(amount) AS amount, SUM(foramount) AS foramount, SUM(number) AS number, SUM(baseamount) AS baseamount
        FROM opening
        GROUP BY [year], [period], [type], cid, serie, account, currency, o1, o2, o3, o4, o5, o6, o7, o8, basecurrency)

    --- Because it only contains a WHEN MATCHED predicate, this merge is essentially just a glorified UPDATE.
    MERGE INTO dbo.containerx AS c
    USING bal ON EXISTS (
        SELECT   c.[year],   c.[period],   c.[type],   c.cid,   c.serie,   c.account,   c.currency,   c.o1,   c.o2,   c.o3,   c.o4,   c.o5,   c.o6,   c.o7,   c.o8,   c.basecurrency
        INTERSECT
        SELECT bal.[year], bal.[period], bal.[type], bal.cid, bal.serie, bal.account, bal.currency, bal.o1, bal.o2, bal.o3, bal.o4, bal.o5, bal.o6, bal.o7, bal.o8, bal.basecurrency)

    --- If the matched balance plus the change comes to 0, delete the row entirely.
    WHEN MATCHED AND c.amount-bal.amount=0
                 AND c.foramount-bal.foramount=0
                 AND c.number-bal.number=0
                 AND c.baseamount-bal.baseamount=0 THEN DELETE

    --- Otherwise, just update the balances:
    WHEN MATCHED THEN
        UPDATE SET c.amount=c.amount-bal.amount,
                   c.foramount=c.foramount-bal.foramount,
                   c.number=c.number-bal.number,
                   c.baseamount=c.baseamount-bal.baseamount

--  OUTPUT $action, deleted.[year], deleted.[type], deleted.[period], deleted.account, deleted.o1, deleted.amount AS [deleted.amount], bal.amount AS [change amount], inserted.amount AS [inserted.amount]
;

    --- Delete the voucher rows and vouchers
    ----------------------------------------
    DELETE r
    FROM #vouchers AS v
    INNER JOIN dbo.vourowx AS r ON v.[year]=r.[year] AND v.serie=r.serie AND v.vouno=r.vouno;

    DELETE vx
    FROM #vouchers AS v
    INNER JOIN dbo.voucherx AS vx ON v.[year]=vx.[year] AND v.serie=vx.serie AND v.vouno=vx.vouno;



    ALTER TABLE dbo.voucherx ENABLE TRIGGER ALL;
    ALTER TABLE dbo.vourowx ENABLE TRIGGER ALL;
    ALTER TABLE dbo.containerx ENABLE TRIGGER ALL;

COMMIT TRANSACTION;

DROP TABLE #vouchers;

GO
BEGIN TRANSACTION;

    EXECUTE dbo.delete_vouchers;
GO
IF (@@TRANCOUNT!=0)
    ROLLBACK TRANSACTION;
