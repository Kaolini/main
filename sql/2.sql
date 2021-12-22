with
	load_data as (
		select
			polis_oms
			, replace(
				lower(
					concat_ws(' ', last_name, first_name, middle_name, coalesce(date_of_birth , '1900-01-01'::date)::varchar)
				), 'ё', 'е'
			) as pacient_compose
			, concat_ws(' ', last_name, first_name, middle_name) as fio
			, date_of_birth as birth_date
			, pacient_type
			, coalesce(m.code, 'Нет информации') as diagnosis_mkb10
			, date_referral
		from integration.current_load_data cld
		left join mkb10 m on m.code = cld.diagnosis_mkb10
	)
	, report_data_oms as (
		select
			oms
			, split_part(string_agg(escort_agreement, '||| ' order by rn), '||| ', 1) as escort_agreement
			, split_part(string_agg(escort_agreement_emias, '||| ' order by rn), '||| ', 1) as escort_agreement_emias
			, split_part(string_agg(mo_from, '||| ' order by rn), '||| ', 1) as mo_from
			, split_part(string_agg(mo_from_district, '||| ' order by rn), '||| ', 1) as mo_from_district
			, split_part(string_agg(assistant, '||| ' order by rn), '||| ', 1) as assistant
--			, split_part(string_agg(source_flag, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1)
			, split_part(string_agg(mo_file, '||| ' order by rn), '||| ', 1) as mo_file
			, split_part(string_agg(emias_file, '||| ' order by rn), '||| ', 1) as emias_file
		from (
			select
				oms
				, case
					when escort_agreement is not null and lower(escort_agreement) = 'да' then 'да'
					when escort_agreement is not null and lower(escort_agreement) != 'да' then 'нет'
				end as escort_agreement
				, escort_agreement_emias
				, mo_from
				, mo_from_district 
				, assistant
--				, source_flag
				, mo_file
				, emias_file
				, row_number () over (partition by oms order by ord desc) as rn
			from integration.last_load_report llr
			where coalesce(emias_comment, mo_comment) != 'Направление из ЦАОП или ЖК.'
		) llr
		group by oms
	)
	, report_data_fio as (
		select
			pacient_compose
			, split_part(string_agg(escort_agreement, '||| ' order by rn), '||| ', 1) as escort_agreement
			, split_part(string_agg(escort_agreement_emias, '||| ' order by rn), '||| ', 1) as escort_agreement_emias
			, split_part(string_agg(mo_from, '||| ' order by rn), '||| ', 1) as mo_from
			, split_part(string_agg(mo_from_district, '||| ' order by rn), '||| ', 1) as mo_from_district
			, split_part(string_agg(assistant, '||| ' order by rn), '||| ', 1) as assistant
--			, split_part(string_agg(source_flag, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1)
			, split_part(string_agg(mo_file, '||| ' order by rn), '||| ', 1) as mo_file
			, split_part(string_agg(emias_file, '||| ' order by rn), '||| ', 1) as emias_file
		from (
			select
				*
				, row_number () over (partition by pacient_compose order by ord desc) as rn
			from (
				select
					replace(
						lower(
							concat_ws(' ', pacient, coalesce(birth_date, '1900-01-01'::date)::varchar)
						), 'ё', 'е'
					) as pacient_compose
					, case
						when escort_agreement is not null and lower(escort_agreement) = 'да' then 'да'
						when escort_agreement is not null and lower(escort_agreement) != 'да' then 'нет'
					end as escort_agreement
					, escort_agreement_emias
					, mo_from
					, mo_from_district
					, assistant
					, source_flag
					, mo_file
					, emias_file
					, ord
				from integration.last_load_report
				where coalesce(emias_comment, mo_comment) != 'Направление из ЦАОП или ЖК.'
			) llr
		) llr2
		group by pacient_compose
	)
	, old_pacient as (
		select
			p.id
			, p.oms
			, p.pacient
			, p.agree_to_assist as old_agree
			, p.id_assist as old_assist
			, string_agg(p.code, ', ' order by p.diag_id) as old_mkb10
		from (
			select
				p1.id
				, p1.oms
				, replace(
						lower(
							concat_ws(' '
								, replace(p1.surname, ' ', '')
								, replace(p1.name, ' ', '')
								, replace(p1.patronymic , ' ', '')
								, coalesce(p1.date_birth, '1900-01-01'::date)::varchar
							)
						), 'ё', 'е') as pacient
				, p1.agree_to_assist
				, pa.id_assist
				, m.code
				, d2.id as diag_id
			from pacient p1
			left join (
				select *
				, row_number () over (partition by id_pacient order by date_out desc) as rn
				from personal_assist
			) pa on pa.id_pacient = p1.id and pa.rn = 1
			left join illness i2 on i2.id_pacient = p1.id 
			left join diagnoses d2 on d2.id_illness = i2.id
			left join mkb10 m on m.id = d2.id_mkb10
			where (p1.date_assist = func_now_utc()::date or p1.id in (select public_pacient_id from integration.current_load_data cld2))
				and d2.date_out >= '3000-01-01 00:00:00'
		) p
		group by p.id, p.oms, p.pacient, p.agree_to_assist, p.id_assist
	)
	, pacients as (
		select
			pacient_type 
			, oms
			, fio
			, birth_date
			, pacient_compose
			, new_date_referral
			, old_mkb10
			, new_mkb10
			, new_mo
			, new_district
			, case
				when emias_file is not null and mo_file is null then 'ЕМИАС'
				when emias_file is not null and mo_file is not null then 'ЕМИАС, МО'
				when emias_file is null and mo_file is not null then 'МО'
			end as source_flag
			, mo_file 
			, old_assist
			, new_assist
			, old_agree
			, case
				when new_agree = 'да' then true
				when new_agree = 'нет' then false
				when new_agree is null and new_assist is not null then true
				when new_agree is null and new_assist is null then null
			end as new_agree
			, case
				when new_agree_emias = 'да' then true
				when new_agree_emias = 'нет' then false
			end as new_agree_emias
		from (
			select
				ld.polis_oms as oms
				, ld.fio as fio
				, ld.birth_date as birth_date
				, ld.pacient_compose
				, ld.diagnosis_mkb10 as new_mkb10
				, ld.pacient_type
				, ld.date_referral as new_date_referral
				, lower(coalesce(rdo.escort_agreement, rdf.escort_agreement, rdo.escort_agreement_emias, rdf.escort_agreement_emias)) as new_agree
				, lower(coalesce(rdo.escort_agreement_emias, rdf.escort_agreement_emias)) as new_agree_emias
				, coalesce(rdo.mo_from, rdf.mo_from) as new_mo
				, coalesce(rdo.mo_from_district, rdf.mo_from_district) as new_district
				, coalesce(rdo.assistant, rdf.assistant) as new_assist
--				, coalesce(rdo.source_flag, rdf.source_flag) as source_flag
				, coalesce(rdo.mo_file, rdf.mo_file) as mo_file 
				, coalesce(rdo.emias_file, rdf.emias_file) as emias_file 
				, op.old_agree
				, op.old_assist
				, op.old_mkb10
			from load_data ld
			left join report_data_oms rdo on rdo.oms = ld.polis_oms
			left join report_data_fio rdf on rdf.pacient_compose = ld.pacient_compose
			left join old_pacient op on op.oms = ld.polis_oms or ld.pacient_compose = op.pacient
		) pre
	)
	, mkb10_oms as (
		select distinct
			llr.oms
			, llr.mkb10
			, coalesce(m.code, 'Нет информации') as good_mkb10
		from (
			select
				oms
				, mkb10
				, integration.func_get_good_mkb(mkb10) as good_mkb10
			from integration.last_load_report
		) llr
		left join mkb10 m on llr.good_mkb10 = m.code
	)
	, mkb10_fio as (
		select distinct
			llr.pacient_compose
			, llr.mkb10
			, coalesce(m.code, 'Нет информации') as good_mkb10
		from (
			select
				replace(
					lower(
						concat_ws(' ', pacient, coalesce(birth_date, '1900-01-01'::date)::varchar)
					), 'ё', 'е'
				) as pacient_compose
				, mkb10
				, integration.func_get_good_mkb(mkb10) as good_mkb10
			from integration.last_load_report
		) llr
		left join mkb10 m on llr.good_mkb10 = m.code
	)
	, problem_stat as (
		select
			ord
			, problem_group
			, problem
			, count(*) as cnt
		from (
			select
				ord
				, split_part(string_agg(new_date_referral::varchar, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1)::date as new_date_referral
				, split_part(string_agg(new_district, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1) as new_district
				, split_part(string_agg(new_mo, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1) as new_mo
				, split_part(string_agg(fio, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1) as fio
				, split_part(string_agg(birth_date::varchar, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1)::date as birth_date
				, pre.oms
				, string_agg(coalesce(mo.mkb10, mf.mkb10), ', ' order by new_mkb10) as new_mkb10
				, split_part(string_agg(problem_group, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1) as problem_group
				, split_part(string_agg(problem, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1) as problem
				, null as space_1
				, null as space_2
				, null as space_3
				, coalesce(
					split_part(string_agg(mo_file, '||| ' order by coalesce(new_date_referral, '3000-01-01')), '||| ', 1)
					, '(пациент поступил только из ЕМИАС)'
				) as file
			from (
				select 1 as ord, 'Требует назначения ПП' as problem_group, 'В файле МО не указан ПП' as problem, p.*
				from pacients p
				where pacient_type in (1,2,3) and source_flag in ('МО', 'ЕМИАС, МО') and new_agree = true and new_assist is null
				union all
				select 2 as ord, 'Требует назначения ПП' as problem_group, 'Указанный в файле МО ПП не обнаружен в системе' as problem, p.*
				from pacients p
				where pacient_type in (1,2,3) and source_flag in ('МО', 'ЕМИАС, МО') and new_agree = true and new_assist is not null and old_assist = 0
				union all
				select 3 as ord, 'Требует назначения ПП' as problem_group, 'Были в данных ЕМИАС, но отсутствуют в МО' as problem, p.*
				from pacients p
				where pacient_type in (1,2,3) and source_flag in ('ЕМИАС') and old_agree = true
				union all
				select 4 as ord, 'Повторные направления' as problem_group, 'Согласие: ранее - "да", сейчас - "нет", сущ. диагноз' as problem, p.*
				from pacients p
				where pacient_type in (4,5,6,7,8,9) and new_agree = false and old_agree = true and position(new_mkb10 in old_mkb10) > 0
				union all
				select 5 as ord, 'Повторные направления' as problem_group, 'Согласие: ранее - "да", сейчас - "нет", новый диагноз' as problem, p.*
				from pacients p
				where pacient_type in (4,5,6,7,8,9) and new_agree = false and old_agree = true and position(new_mkb10 in old_mkb10) = 0
				union all
				select 6 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "да", МО - пациент отсутствует' as problem, p.*
				from pacients p
				where source_flag in ('ЕМИАС') and new_agree_emias = true
				union all
				select 7 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "нет", МО - пациент отсутствует' as problem, p.*
				from pacients p
				where source_flag in ('ЕМИАС') and new_agree_emias = false
				union all
				select 8 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "да", МО - "нет"' as problem, p.*
				from pacients p
				where source_flag in ('ЕМИАС, МО') and new_agree_emias = true and new_agree = false
				union all
				select 9 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "нет", МО - "да"' as problem, p.*
				from pacients p
				where source_flag in ('ЕМИАС, МО') and new_agree_emias = false and new_agree = true
				union all
				select 10 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - пациент отсутствует, МО - "да"' as problem, p.*
				from pacients p
				where source_flag in ('МО') and new_agree = true
				union all
				select 11 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - пациент отсутствует, МО - "нет"' as problem, p.*
				from pacients p
				where source_flag in ('МО') and new_agree = false
			) pre
			left join mkb10_oms mo on mo.oms = pre.oms and mo.good_mkb10 = pre.new_mkb10
			left join mkb10_fio mf on mf.pacient_compose = pre.pacient_compose and mf.good_mkb10 = pre.new_mkb10
			group by ord, pre.oms
		) pl
		group by ord, problem_group, problem
		order by ord
	)
select
	main.*
	, coalesce(ps.cnt, 0) as cnt
from (
	select 1 as ord, 'Требует назначения ПП' as problem_group, 'В файле МО не указан ПП' as problem
	union all
	select 2 as ord, 'Требует назначения ПП' as problem_group, 'Указанный в файле МО ПП не обнаружен в системе' as problem
	union all
	select 3 as ord, 'Требует назначения ПП' as problem_group, 'Были в данных ЕМИАС, но отсутствуют в МО' as problem
	union all
	select 4 as ord, 'Повторные направления' as problem_group, 'Согласие: ранее - "да", сейчас - "нет", сущ. диагноз' as problem
	union all
	select 5 as ord, 'Повторные направления' as problem_group, 'Согласие: ранее - "да", сейчас - "нет", новый диагноз' as problem
	union all
	select 6 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "да", МО - пациент отсутствует' as problem
	union all
	select 7 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "нет", МО - пациент отсутствует' as problem
	union all
	select 8 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "да", МО - "нет"' as problem
	union all
	select 9 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - "нет", МО - "да"' as problem
	union all
	select 10 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - пациент отсутствует, МО - "да"' as problem
	union all
	select 11 as ord, 'Расхождения в "согласии" от ЕМИАС и МО' as problem_group, 'ЕМИАС - пациент отсутствует, МО - "нет"' as problem
) main
left join problem_stat ps on ps.ord = main.ord
order by ord