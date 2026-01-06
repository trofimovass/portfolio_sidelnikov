--Задание1
select count(project_id)
from project 
where sign_date::date between '2023-01-01' and '2023-12-31';

--Задание 2
select (sum(age(current_date, p.birthdate))) as total_age
from person p
join employee e on p.person_id = e.person_id
where e.hire_date::date between '2022-01-01' and '2022-12-31';

--Задание 3
select concat_ws(' ', p.first_name, p.last_name) as "Фамилия и имя", e.hire_date as "Дата найма"
from employee e
join person p on e.person_id = p.person_id
where p.last_name like 'М%' and length(p.last_name)=8
order by e.hire_date 
limit 1;


--Задание 4
-- 4 уволенных сотрудника, но каждый из них задействован хотя бы в одном проекте, поэтому результат 0
-- Доп проверка, что сотрудники не являются руководителями проектов
select coalesce(avg(date_part('year', age(current_date, p.birthdate))) ,0) as average_age
from person p
join employee e on p.person_id = e.person_id
where e.dismissal_date is not null
and not exists  (
    select 1 
    from project pr 
    where e.employee_id = ANY(pr.employees_id) or e.employee_id = pr.project_manager_id
);

--Задание 5
--Находим Россию, находим Жуковский в России, находим все адреса в Жуковском, находим клиентов с этими адресами, находим проекты этих клиентов, суммируем платежи этих проектов
--Выбираем только фактически полученные платежи
select sum(pp.amount)
from project_payment pp
where pp.project_id in (
    select distinct p.project_id
    from project p
    join customer c ON p.customer_id = c.customer_id
    join address a ON c.address_id = a.address_id
    join city ci ON a.city_id = ci.city_id
    join country co ON ci.country_id = co.country_id
    where ci.city_name = 'Жуковский' and co.country_name = 'Россия'
)
and pp.fact_transaction_timestamp is not null;

--Задание 6
--Считаем общую стоимость по завершенным проектам
--Вычисляем премию, присваиваем ранг каждому руководителю, находим ФИО
--Выбираем ранг=1, если таких менеджеров несколько, то они все попадут в выборку
--Сортируем по убыванию  
WITH sum_total as ( 
select pr.project_manager_id, sum(pr.project_cost) as total_cost
from project pr
where pr.status = 'Завершен'
group by pr.project_manager_id
),
manager_bonus as (
select st.project_manager_id, p.full_fio, (st.total_cost) * 0.01 as "Размер бонуса",
       dense_rank() over (order by (st.total_cost) * 0.01 desc) as bonus_rank
from sum_total st
join employee e on st.project_manager_id = e.employee_id 
join person p on e.person_id = p.person_id
)
select project_manager_id, full_fio, "Размер бонуса"
from manager_bonus
where bonus_rank = 1
order by "Размер бонуса" desc;

--Задание 7
--Вычисляем месяц платежа, выбираем Авансовый платеж
--Вычисляем накопительно сумму авансовых платежей за месяц, упорядочиваем по дате
--Фильтруем строки с превышением 30 000 000, при помощи lag находим первое превышение условия
--В основном запросе выводим значения, соответствующие условию (null нужен, если первая строка подойдет под условие), сортируем, выводим результат
with cte1 as (
select plan_payment_date::date, date_trunc('month', plan_payment_date::date) as month, amount
from project_payment
where payment_type = 'Авансовый'
),
cte2 as (
select plan_payment_date::date, month,
sum(amount) over (partition by month order by  plan_payment_date::date) as cumulative_sum
from cte1
),
cte3 as (
select plan_payment_date::date, month, cumulative_sum,
lag(cumulative_sum) over (partition by  month order by  plan_payment_date::date) as first_cumulative_sum
from cte2
where cumulative_sum > 30000000
)
select plan_payment_date::date as "дата", cumulative_sum as "накопление"
from cte3
where (first_cumulative_sum is null or first_cumulative_sum <= 30000000)
order by month, plan_payment_date;


--Задание 8
--Находим подразделения с id=17
--Находим дочерние подразделения
--Присоединяем по unit_id, чтобы найти должности
--Присоединяем сотрудников на этих позициях
--Считаем фактический оклад (зарплату*ставку)
with recursive sub_units as (
select unit_id, parent_id, unit_name, unit_type
from company_structure
where unit_id = 17
    
union all
    
select cs.unit_id, cs.parent_id, cs.unit_name, cs.unit_type
from company_structure cs
join sub_units su on cs.parent_id = su.unit_id
)
select sum(ep.salary * ep.rate) as total_salary
from sub_units su
join position p on su.unit_id = p.unit_id
join employee_position ep on p.position_id = ep.position_id;


--Задание 9
--Группируем фактические платежи по годам, присваиваем порядковые номера отдельно по годам
--Исключаем записи с NULL
--Фильтруем каждую 5-ю запись и считаем скользящее среднее (2 до и 2 после)
--Суммируем все скользящие средние в одно общее значение
--Считаем стоимость проектов по годам
--Выводим результат: годы, где суммарная стоимость проектов меньше общего скользящего среднего
with payments_rank as (
select date_trunc('year', fact_transaction_timestamp) as year, amount,
row_number() over (partition by date_trunc('year', fact_transaction_timestamp) order by fact_transaction_timestamp) as payment_rank
from project_payment
where fact_transaction_timestamp is not null
),
filtered_payments as (
select year, amount, payment_rank,
avg(amount) over (order by payment_rank rows between 2 preceding and 2 following) as moving_avg_amount
from payments_rank
where payment_rank % 5 = 0
),
total_moving_avg as (
select sum(moving_avg_amount) as total_sum_moving_avg
from filtered_payments
),
yearly_project_costs as (
select date_trunc('year', sign_date) as year,
sum(project_cost) as sum_project_cost
from project
group by year
)
select extract(year from ypc.year) as year, ypc.sum_project_cost
from yearly_project_costs ypc
cross join total_moving_avg tma
where ypc.sum_project_cost < tma.total_sum_moving_avg
order by year;


--Задание 10
--Сначала выбираем id проекта, платеж, нумеруем фактические платежи, сортируем их по убыванию
--Получаем список с платежами
--Через id контрагента соединяем таблицы customer, customer_type_of_work и type_of_work для объединения типов работ
--Получаем строку со всеми работами
--Из таблицы project берем id проекта, его название, присоединяем из подзапроса последний фактический платеж
--Присоединяем ФИО по руководителю проекта
--Присоединяем название контрагента
--Присоединяем из подзапроса виды работ

DROP MATERIALIZED VIEW project_report;


create materialized view project_report_new as
with last_payments as (
select pp.project_id, pp.amount, pp.fact_transaction_timestamp,
row_number() over (partition by pp.project_id order by pp.fact_transaction_timestamp desc) as rn
from project_payment pp
where pp.fact_transaction_timestamp is not null
),
customer_work_types as (
select c.customer_id,
string_agg(tow.type_of_work_name, ', ' order by tow.type_of_work_name) as work_types
from customer c
left join customer_type_of_work ctow on c.customer_id = ctow.customer_id
left join type_of_work tow on ctow.type_of_work_id = tow.type_of_work_id
group by c.customer_id
)
select p.project_id, p.project_name, lp.fact_transaction_timestamp as last_payment_date,
    lp.amount as last_payment_amount,
    per.full_fio as project_manager_name,
    c.customer_name,
    cwt.work_types as customer_work_types
from project p
left join last_payments lp on p.project_id = lp.project_id and lp.rn = 1
left join employee e on p.project_manager_id = e.employee_id
left join person per on e.person_id = per.person_id
left join customer c on p.customer_id = c.customer_id
left join customer_work_types cwt on c.customer_id = cwt.customer_id;


SELECT * FROM project_report_new;