-- Referente a tabela do Resiliencia MTP_TAG
select 
    CD_TAG TAG
    ,t.ID_OSA
    ,CODIGO_EMISSOR
    ,PLACA
    ,LIBERADO
    ,ID_CATEGORIA
    ,t.NUMERO_SERIE SERIE
    ,null SEQUENCIAL
    ,null BL_ISENTO
    ,p.CD_PRACA LOCAL
    ,t.DH_INVENTARIO DH_RECEBIMENTO
    ,t.DH_ATUALIZACAO DH_ATUALIZACAO
    ,0 ID_SEQ
    ,0 BL_EXPORTADO
from 
    TAG t
inner join TAG_ISENTO ti on
    t.PLACA = ti.DS_PLACA
inner join OSA_CONCESSAO_PRACA ocp on
    ocp.ID_OSA = T.ID_OSA
inner join PRACA p on
    ocp.CD_PRACA = p.CD_PRACA