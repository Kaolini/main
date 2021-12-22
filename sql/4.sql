with
	load_data as (
		select
			polis_oms
			, lower(concat_ws(' ', last_name, first_name, middle_name, coalesce(date_of_birth , '1900-01-01'::date)::varchar)) as pacient_compose
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
			, escort_agreement
			, escort_agreement_emias
			, mo_from
			, mo_from_district
			, assistant
			, source_flag
			, mo_file 
		from (
			select
				oms
				, escort_agreement
				, escort_agreement_emias
				, mo_from
				, mo_from_district 
				, assistant
				, source_flag
				, mo_file 
				, row_number () over (partition by oms order by ord desc) as rn
			from integration.last_load_report llr
			where coalesce(emias_comment, mo_comment) != 'Направление из ЦАОП или ЖК.'
		) llr
		where rn = 1
	)
	, report_data_fio as (
		select
			pacient_compose
			, escort_agreement
			, escort_agreement_emias
			, mo_from
			, mo_from_district
			, assistant
			, source_flag
			, mo_file 
		from (
			select
				*
				, row_number () over (partition by pacient_compose order by ord desc) as rn
			from (
				select
					replace(
						lower(
							concat_ws(' '
								, pacient
								, coalesce(birth_date, '1900-01-01'::date)::varchar
							)
						), 'ё', 'е'
					) as pacient_compose
					, escort_agreement
					, escort_agreement_emias
					, mo_from
					, mo_from_district
					, assistant
					, source_flag
					, mo_file 
					, ord
				from integration.last_load_report
				where coalesce(emias_comment, mo_comment) != 'Направление из ЦАОП или ЖК.'
			) llr
		) llr2
		where rn = 1
	)
	, old_pacient as (
		select
			p.id
			, p.oms
			, p.pacient
			, p.pacient_2
			, p.agree_to_assist as old_agree
			, p.assistant as old_assist
			, p.date_assist
			, p.load_type
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
				, replace(
							concat_ws(' '
								, replace(p1.surname, ' ', '')
								, replace(p1.name, ' ', '')
								, replace(p1.patronymic , ' ', '')
								, coalesce(p1.date_birth, '1900-01-01'::date)::varchar
							)
						, 'ё', 'е') as pacient_2
				, p1.agree_to_assist
				, p1.date_assist
				, case when p1.date_assist < '2021-03-10' then 'Миграция' else 'Интеграция' end as load_type
				, concat_ws(' ', a.surname, a."name", a.patronymic) as assistant
				, m.code
				, d2.id as diag_id
			from pacient p1
			left join (
				select *
				, row_number () over (partition by id_pacient order by date_out desc) as rn
				from personal_assist
			) pa on pa.id_pacient = p1.id and pa.rn = 1
			left join assistant a on a.id = pa.id_assist 
			left join illness i2 on i2.id_pacient = p1.id 
			left join diagnoses d2 on d2.id_illness = i2.id
			left join mkb10 m on m.id = d2.id_mkb10
			where (p1.date_assist = func_now_utc()::date or p1.id in (select public_pacient_id from integration.current_load_data cld2))
				and d2.date_out >= '3000-01-01 00:00:00'
		) p
		group by p.id, p.oms, p.pacient, p.pacient_2, p.agree_to_assist, p.assistant, p.load_type, p.date_assist
	)
	, pacients as (
		select
			pacient_type 
			, oms
			, fio
			, birth_date
			, new_date_referral
			, old_mkb10
			, new_mkb10
			, new_mo
			, new_district
			, source_flag
			, mo_file 
			, old_assist
			, new_assist
			, old_agree
			, date_assist
			, load_type
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
			, old_pacient
			, old_oms
		from (
			select
				ld.polis_oms as oms
				, ld.fio as fio
				, ld.birth_date as birth_date
				, ld.diagnosis_mkb10 as new_mkb10
				, ld.pacient_type
				, ld.date_referral as new_date_referral
				, lower(coalesce(rdo.escort_agreement, rdf.escort_agreement, rdo.escort_agreement_emias, rdf.escort_agreement_emias)) as new_agree
				, lower(coalesce(rdo.escort_agreement_emias, rdf.escort_agreement_emias)) as new_agree_emias
				, coalesce(rdo.mo_from, rdf.mo_from) as new_mo
				, coalesce(rdo.mo_from_district, rdf.mo_from_district) as new_district
				, coalesce(rdo.assistant, rdf.assistant) as new_assist
				, coalesce(rdo.source_flag, rdf.source_flag) as source_flag
				, coalesce(rdo.mo_file, rdf.mo_file) as mo_file 
				, op.old_agree
				, op.old_assist
				, op.old_mkb10
				, op.date_assist
				, op.load_type
				, op.pacient_2 as old_pacient
				, op.oms as old_oms
			from load_data ld
			left join report_data_oms rdo on rdo.oms = ld.polis_oms
			left join report_data_fio rdf on rdf.pacient_compose = ld.pacient_compose
			left join old_pacient op on op.oms = ld.polis_oms or ld.pacient_compose = op.pacient
		) pre
	)
select * from (
	select
		old_oms as oms_1
		, old_pacient
		, case when old_agree = true then 'Да' else 'Нет' end
		, date_assist
		, load_type
		, old_assist
		, case
			when pacient_type in (12, 15) then replace(replace(replace(old_mkb10, new_mkb10||', ', ''), ', '||new_mkb10, ''), new_mkb10, '')
			else old_mkb10
		end
		, source_flag
		, oms
		, concat_ws(' ', fio, birth_date::varchar) as pacient
		, case when new_agree = true then 'Да' else 'Нет' end
		, new_mkb10
		, case
			when pacient_type not in (12, 13, 15) and position(new_mkb10 in old_mkb10) > 0 then 'Существующий диагноз'
			when pacient_type in (12, 15) and position(new_mkb10 in old_mkb10) > 0 then 'Новый диагноз'
			when pacient_type = 13 and position(new_mkb10 in old_mkb10) > 0 then 'Существующий + Новый диагнозы'
			else 'Новый диагноз'
		end
		, case
			when pacient_type in (11, 12, 13) then 'Согласие изменено на "Да"'
			when pacient_type in (15) then 'Загружена новая болезнь'
			when pacient_type in (10) then 'Добавлена информация о ПП'
			when pacient_type in (4,5,6,7,8,9, 14) then 'Пациент не загружен'
		end
	from pacients
	where pacient_type not in (1,2,3)
) main
order by oms_1