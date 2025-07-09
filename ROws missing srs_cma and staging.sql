USE [sipr];
GO

SELECT 
    cma.cma_ayrc,
    cma.cma_crsc,
    cma.cma_blok,
    cma.cma_cboo,
    cma.cma_modc,
    cma.cma_modo,
    cma.cma_ayr1,
    cma.cma_pslc,
    'srs_cma' AS SourceTable
FROM dbo.srs_cma cma
LEFT JOIN dbo.srs_cma_STAGING stg
    ON cma.cma_ayrc = stg.cma_ayrc
    AND cma.cma_crsc = stg.cma_crsc
    AND cma.cma_blok = stg.cma_blok
    AND cma.cma_cboo = stg.cma_cboo
    AND cma.cma_modc = stg.cma_modc
    AND cma.cma_modo = stg.cma_modo
    AND cma.cma_ayr1 = stg.cma_ayr1
    AND cma.cma_pslc = stg.cma_pslc
WHERE stg.cma_ayrc IS NULL

UNION ALL

SELECT 
    stg.cma_ayrc,
    stg.cma_crsc,
    stg.cma_blok,
    stg.cma_cboo,
    stg.cma_modc,
    stg.cma_modo,
    stg.cma_ayr1,
    stg.cma_pslc,
    'srs_cma_STAGING' AS SourceTable
FROM dbo.srs_cma_STAGING stg
LEFT JOIN dbo.srs_cma cma
    ON stg.cma_ayrc = cma.cma_ayrc
    AND stg.cma_crsc = cma.cma_crsc
    AND stg.cma_blok = cma.cma_blok
    AND stg.cma_cboo = cma.cma_cboo
    AND stg.cma_modc = cma.cma_modc
    AND stg.cma_modo = cma.cma_modo
    AND stg.cma_ayr1 = cma.cma_ayr1
    AND stg.cma_pslc = cma.cma_pslc
WHERE cma.cma_ayrc IS NULL;
