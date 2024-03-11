with payments as (
	select
		payment_date
		,user_id
		,revenue_amount_usd
		,date(date_trunc('month',payment_date)) as payment_month
	from
		games_payments
), 
churn as (
	select 
		payment_month
		,user_id
		,case when lead (cast (date_trunc('month', payment_month) as date)) over 
			(partition  by user_id 
				order by payment_month) is null
    	or lead (cast (date_trunc('month', payment_month) as date)) over 
    		(partition  by user_id order by payment_month) != cast(date_trunc('month', payment_month) as date) + interval '1 month'
    	then cast(date_trunc('month', payment_month) as date) + interval '1 month'
    	end as churn_month
    	,case when lead (cast (date_trunc('month', payment_month) as date)) over 
			(partition  by user_id 
				order by payment_month) is null
    	or lead (cast (date_trunc('month', payment_month) as date)) over 
    		(partition  by user_id order by payment_month) != cast(date_trunc('month', payment_month) as date) + interval '1 month'
    	then 1
    	else 0
    	end as churned_user
    	,case when cast((payment_month + interval  '1 month') :: text as date)  = lead (payment_month) over (
			partition  by user_id
				order by payment_month)
			then 0
			else sum(revenue_amount_usd)
			end as churned_revenue
	from payments as p
	group by payment_month, user_id
	order by payment_month, user_id
),
calc_payments as (
	select
		payment_month
		,user_id
		,sum(revenue_amount_usd) as "MRR"
		,cast( date_trunc ( 'month', min(payment_month) over 
			(partition  by user_id 
				order by payment_month)) as date) 
					as first_payment_month
		,case when payment_month = cast( date_trunc ( 'month', min(payment_month) over 
			(partition  by user_id 
				order by payment_month)) as date)
		 	then 1
			else 0
			end as "New Paid Users"
		,case when payment_month = cast( date_trunc ( 'month', min(payment_month) over 
			(partition  by user_id 
				order by payment_month)) as date)
		 	then sum(revenue_amount_usd)
			else 0
			end as "New MRR"
		,case when payment_month !=  substring((lag (payment_month) over (
			partition  by user_id
				order by payment_month
				) + interval  '1 month') :: text, 1, 10) :: date 
			then 0
			when sum(revenue_amount_usd) > lag (sum(revenue_amount_usd)) over (
			partition by user_id
				order by payment_month)
			then sum(revenue_amount_usd) - coalesce  (lag (sum(revenue_amount_usd)) over (
			partition  by user_id
				order by payment_month), 0)
			else 0 
			end as "Expansion MRR"
		,case when payment_month !=  substring((LAG (payment_month) over (
			partition  by user_id
				order by payment_month
				) + interval  '1 month') :: text, 1, 10) :: date 
			then 0
			when sum(revenue_amount_usd) < lag (sum(revenue_amount_usd)) over (
			partition by user_id
				order by payment_month)
			then sum(revenue_amount_usd) - coalesce  (lag (sum(revenue_amount_usd)) over (
			partition  by user_id
				order by payment_month), 0)
			else 0 
			end as "Contraction MRR"
		,date_part('month', age(first_value  (payment_month) over (
			partition by user_id
				order by payment_month desc ),
			first_value (payment_month) over (
			partition by user_id
			))) as "LT"
	from payments as p
	group by payment_month, user_id
	order by payment_month, user_id
), all_data as (
	select 
		cp.payment_month
		,cp.user_id
		,cp."MRR"
		,cp.first_payment_month
		,cp."New Paid Users"
		,cp."New MRR"
		,cp."Expansion MRR"
		,cp."Contraction MRR"
		,cp."LT"
		,ch.churn_month
		,ch.churned_user
		,ch.churned_revenue
	from calc_payments as cp
	left join churn as ch on ch.user_id = cp.user_id and cp.payment_month=ch.payment_month
)
select 
	al.payment_month
	,al.user_id
	,gp.game_name
	,gp.language
	,gp.age
	,al."MRR"
	,al.first_payment_month
	,al."New Paid Users"
	,al."New MRR"
	,al."Expansion MRR"
	,al."Contraction MRR"
	,al."LT"
	,al.churn_month
	,al.churned_user
	,al.churned_revenue
from all_data as al
left join games_paid_users as gp on gp.user_id = al.user_id