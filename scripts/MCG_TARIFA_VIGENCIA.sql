-- Referente a tabela do Resiliencia MCG_TARIFA_VIGENCIA
select 
    tv.CD_TARIFA_VIGENCIA ID_TARIFA_VIGENCIA
    ,tv.DH_INICIO_VIGENCIA DH_INICIO
    ,tv.DH_FIM_VIGENCIA DH_FIM
    ,0 TARIFA_BASICA
    ,p.CD_CONCESSAO ID_CONCESSAO
    ,0 VERSAO -- Não achei campo para esse
    ,tv.DH_ATUALIZACAO DH_ATUALIZACAO
    ,'' BL_EXPORTADO -- BL_LIBERADO?
    ,null DH_EXPORTADO
    ,0 CD_STATUS
    ,getdate() DH_TIMESTAMP
from TARIFA_VIGENCIA tv
inner join PRACA p on
    tv.CD_PRACA = p.CD_PRACA
