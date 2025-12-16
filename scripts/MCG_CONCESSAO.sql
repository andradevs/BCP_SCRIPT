-- Referente a tabela do Resiliencia MCG_CONCESSAO

select 
    c.CD_CONCESSAO ID_CONCESSAO
    ,c.NM_CONCESSAO NOME
    ,ocp.ID_OSA ID_CONCESSAO_OSA
    ,'' ID_CONCESSAO_DBTRANS -- não achei nenhum campo 
    ,isap.CD_CONCESSAO ID_CONCESSAO_SAP
    ,c.IP_CONCESSAO IP_MENSAGERIA
    ,'' RAZAO_SOCIAL -- não sei ao certo qual campo entra aqui
    ,c.NU_CNPJ_CONCESSAO NU_CNPJ
    ,c.NM_LOGRADOURO
    ,c.TL_TELEFONE NU_TELEFONE
    ,c.BL_PORTAL_PAGAMENTO_ONLINE
    ,c.CD_STATUS
    ,getdate() DH_TIMESTAMP
from 
    CONCESSAO c 
inner join OSA_CONCESSAO_PRACA ocp on
    ocp.CD_CONCESSAO = c.CD_CONCESSAO 
inner join PRACA p on
    p.CD_PRACA = ocp.CD_PRACA
inner join INTEGRACAO_SAP isap on
    isap.CD_PRACA = p.CD_PRACA