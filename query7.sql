with cte_1 as (
select 
	fabd.ad_date,
	'Facebook Ads' as media_source,
	fa.adset_name,
	fc.campaign_name,
	fabd.url_parameters,
	coalesce(spend,0) as spend,
	coalesce(impressions,0) as impressions,
	coalesce(reach,0) as reach,
	coalesce(clicks,0) as clicks,
	coalesce (leads,0) as leads,
	coalesce(value,0) as value
from facebook_ads_basic_daily fabd 
left join facebook_adset fa on fabd.adset_id = fa.adset_id
left join facebook_campaign fc on fabd.campaign_id = fc.campaign_id
union all
select
	gabd.ad_date,
	'Google Ads' as media_source,
	gabd.adset_name,
	gabd.campaign_name,
	gabd.url_parameters,
	coalesce(spend,0) as spend,
	coalesce(impressions,0) as impressions,
	coalesce(reach,0) as reach,
	coalesce(clicks,0) as clicks,
	coalesce (leads,0) as leads,
	coalesce(value,0) as value
from google_ads_basic_daily gabd 
), 
cte_2 as (
select 
	date(date_trunc('month', cte_1.ad_date)) as ad_month,
	nullif(lower(substring(cte_1.url_parameters from 'utm_campaign=([^&]+)')), 'nan') as utm_campaign,
		case
			when sum(cte_1.clicks) = 0 then 0
			else sum(cte_1.clicks :: float)/sum(cte_1.impressions)
		end as CTR,
		case 
			when sum(cte_1.clicks) = 0 then 0
			else sum(cte_1.spend :: float)/sum(cte_1.clicks)
		end as CPC,
		case 
			when sum(cte_1.impressions) = 0 then 0
			else sum(cte_1.spend :: float)/sum(cte_1.impressions) * 1000
		end  as CPM,
		case 
			when sum(cte_1.spend) = 0 then 0
			else (sum(cte_1.value :: float)-sum(cast(cte_1.spend as float)))/sum(cte_1.spend)
		end as ROMI
from cte_1 
group by 
	ad_month,
	utm_campaign
)
,
cte_3 as (
    select
        cte_2.*,
        lag(CTR) over (partition by utm_campaign order by ad_month) as prev_CTR,
        lag(CPM) over (partition by utm_campaign order by ad_month) as prev_CPM,
        lag(ROMI) over (partition by utm_campaign order by ad_month) as prev_ROMI
    from cte_2
)
select
    ad_month,
    utm_campaign,
    CTR,
    CPM,
    ROMI,
    case 
        when prev_CTR is not null then ((CTR - prev_CTR) / prev_CTR) * 100
        else null
    end as CTR_change_pct,
    case 
        when prev_CPM is not null then ((CPM - prev_CPM) / prev_CPM) * 100
        else null
    end as CPM_change_pct,
    case 
        when prev_ROMI is not null then ((ROMI - prev_ROMI) / prev_ROMI) * 100
        else null
    end as ROMI_change_pct
from cte_3
;

