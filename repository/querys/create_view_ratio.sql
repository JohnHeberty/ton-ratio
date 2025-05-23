CREATE OR REPLACE VIEW view_ratio AS
with ncm_to_sh4 as ( -- CORRIGINDO NCM PARA SH4
    select m.co_ano, m.co_ncm, RPAD(LEFT(CAST(m.co_ncm AS VARCHAR), 4), 4, '0') AS sh4, m.co_pais, m.sg_uf_ncm, m.vl_fob  
    from ncm_exp m
),
sh4_ncm_group as ( -- AGRUPANDO AS EXPORTAÇÕES DE NCM GERAL POR (ANO, SH4, MES, PAIS, UF)
    select m.co_ano, m.sh4, m.co_pais, m.sg_uf_ncm, SUM(m.vl_fob) as vl_fob
    from ncm_to_sh4 m
    group by (m.co_ano, m.sh4, m.co_pais, m.sg_uf_ncm)
),
sh4_mun_group as ( -- AGRUPANDO AS EXPORTAÇÕES DE MUNICIIOS POR (ANO, SH4, MES, PAIS, UF)
    select m.co_ano, RPAD(LEFT(CAST(m.sh4 AS VARCHAR), 4), 4, '0') as sh4, m.co_pais, m.sg_uf_mun, m.co_mun, SUM(m.vl_fob) as vl_fob
    from mun_exp m
    group by (m.co_ano, m.sh4, m.co_pais, m.sg_uf_mun, m.co_mun)
),
ncm_join_mun as ( -- UNINDO O AGRUPAMENTO GERAL POR NCM COM COM AGRUPAMENTO DE MUNICÍPIO
    select n.co_ano, n.sh4, n.co_pais, n.sg_uf_mun as sg_uf, n.co_mun, n.vl_fob as vl_fob_mun, m.vl_fob as vl_fob_exp
    from sh4_mun_group n
    left join sh4_ncm_group m
    on (n.co_ano = m.co_ano and TRIM(n.sh4) = TRIM(m.sh4) and n.co_pais = m.co_pais and n.sg_uf_mun = m.sg_uf_ncm)
    where 
    	m.vl_fob is not null
),
ratio_vl_fob_mun_ncm as ( -- CALCULANDO AS PORCENTAGENS
	select co_ano, sh4, co_pais, sg_uf, co_mun, vl_fob_mun, vl_fob_exp, ((vl_fob_mun/vl_fob_exp)*100) as ratio_percent
 	from ncm_join_mun
),
ratio_with_desc as ( -- COLOCANDO A DESCRIÇÃO PELO SH4, MAS A ALGUMAS DESCRIÇÃO VAZIAS
	select r.co_ano, r.sh4, r.co_pais, r.sg_uf, r.co_mun, r.vl_fob_mun, r.vl_fob_exp, r.ratio_percent, nv.descricao
	from ratio_vl_fob_mun_ncm r
	left join ncm_vigentes nv on (r.sh4 = nv.codigo)
),
ncm_and_sh4 as ( -- VALORES UNICOS DE SH4 PARA NCM DISTRIBUIDO POR UF/PAIS/ANO
	select distinct co_ano, co_ncm, sh4, co_pais, sg_uf_ncm as sg_uf
	from ncm_to_sh4 -- sh4_ncm_group ncm_to_sh4
)/*,
final as (
	select r.co_ano, exp.co_ncm, r.sh4, r.co_pais, r.sg_uf, r.co_mun, r.vl_fob_mun, r.vl_fob_exp, r.ratio_percent, r.descricao
	from ratio_with_desc r
	left join ncm_and_sh4 exp
	on (r.co_ano = exp.co_ano and r.sh4 = exp.sh4 and exp.co_pais = r.co_pais and exp.sg_uf_ncm = r.sg_uf)
)*/
select *
from ratio_with_desc; -- ratio_with_desc: 29, ncm_and_sh4: 12, sh4_mun_group: 29
-- where
-- 	co_ano = 2023 and sh4 = '7318' and co_pais = '845' and sg_uf = 'SC'
--order by co_mun desc

/* -- MUITAS LINHAS DUPLICADAS
select *
from final
where
	sh4 = '7318' and co_ano = 2023 and co_pais = '845' and sg_uf = 'SC'
 */

/* -- LINHA 22 DUPLICADA COM A LINHA 20
select *
from final m
where
	m.co_ano = 2019
	--and m.sh4 = '1105'
	and m.sg_uf = 'MG'
	and m.co_pais = '455'
order by (co_ano, co_ncm, sh4, co_pais, sg_uf, co_mun, ratio_percent)
*/
