select * from (
	select 1 as ord, 'Всего новых пациентов, в т.ч.:', cld.cnt + llr.cnt
	from (
		select count(distinct polis_oms) as cnt
		from integration.current_load_data
	) cld
	cross join (
		select count(distinct oms) as cnt
		from integration.last_load_report llr
		where emias_comment = 'Направление из ЦАОП или ЖК.'
	) llr
	union all
	select 2 as ord, 'Загружено, в т.ч.:', count(distinct polis_oms)
	from integration.current_load_data cld
	where pacient_type in (1,2,3)
	union all
	select 3 as ord, 'Дали согласие на сопровождение (сгенерировано заданий), в т.ч.:', count(distinct polis_oms)
	from integration.current_load_data cld
	where pacient_type in (1,2,3) and lower(escort_agreement) = 'да'
	union all
	select 4 as ord, 'Привязано к ПП', count(distinct cld.polis_oms)
	from integration.current_load_data cld
	left join pacient p on p.oms = cld.polis_oms
	left join (select *, row_number () over (partition by id_pacient order by date_out desc) as rn from personal_assist) pa on pa.id_pacient = p.id and pa.rn = 1
	where cld.pacient_type in (1,2,3) and lower(cld.escort_agreement) = 'да' and pa.id_assist != 0
	union all
	select 5 as ord, 'Не привязано к ПП (на суперпользователе)', count(distinct cld.polis_oms)
	from integration.current_load_data cld
	left join pacient p on p.oms = cld.polis_oms
	left join (select *, row_number () over (partition by id_pacient order by date_out desc) as rn from personal_assist) pa on pa.id_pacient = p.id and pa.rn = 1
	where cld.pacient_type in (1,2,3) and lower(cld.escort_agreement) = 'да' and pa.id_assist = 0
	union all
	select 6 as ord, 'Не дали согласие на сопровождение', count(distinct polis_oms)
	from integration.current_load_data cld
	where pacient_type in (1,2,3) and lower(escort_agreement) = 'нет'
	union all
	select 7 as ord, 'Не загружено, в т.ч.:', cld.cnt + llr.cnt
	from (
		select count(distinct polis_oms) as cnt
		from integration.current_load_data
		where pacient_type not in (1,2,3)
	) cld
	cross join (
		select count(distinct oms) as cnt
		from integration.last_load_report
		where emias_comment = 'Направление из ЦАОП или ЖК.'
	) llr
	union all
	select 8 as ord, 'Направления из ЦАОП/ЖК, в т.ч.:', count(distinct oms)
	from integration.last_load_report llr
	where emias_comment = 'Направление из ЦАОП или ЖК.'
	union all
	select 9 as ord, 'Из ЦАОП', count(distinct oms)
	from integration.last_load_report llr
	where emias_comment = 'Направление из ЦАОП или ЖК.' and mo_from_emias_id in (select distinct id_emias from organization o2 where caop = true)
	union all
	select 10 as ord, 'Из ЖК (по косвенным признакам)',  llr.cnt - llr2.cnt
	from (
		select count(distinct oms) as cnt
		from integration.last_load_report
		where emias_comment = 'Направление из ЦАОП или ЖК.'
	) llr
	cross join (
		select count(distinct oms) as cnt
		from integration.last_load_report llr
		where emias_comment = 'Направление из ЦАОП или ЖК.' and mo_from_emias_id in (select distinct id_emias from organization o2 where caop = true)
	) llr2
	union all
	select 11 as ord, 'Повторно направленные пациенты, в т.ч.:', count(distinct polis_oms)
	from integration.current_load_data cld
	where pacient_type in (4,5,6, 7,8,9, 11,12,13 ,14,15)
	union all
	select 12 as ord, 'Создано заданий для повторных пациентов', count(distinct polis_oms)
	from integration.current_load_data cld
	where pacient_type in (11,12,13 ,15)
) main order by ord