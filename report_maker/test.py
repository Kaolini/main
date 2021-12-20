import core

t ={"Сводка": [['B4', 'B15'], ['C20', 'C30']],
 "Проблемы": [['A2', 'N10000']],
 'Исходные данные':[['A2', 'AG10000']]}

xlsx = core.Core()
xlsx.create_reports()



# # file = xlsx.get_xml_file(r'C:\Users\mixaz\Desktop\Шаблоны\Отчет о загрузке от 07.12.2021.xlsx')
#
# clear = xlsx.clear_values(r'C:\Users\mixaz\Desktop\Шаблоны\Отчет о загрузке от 07.12.2021.xlsx', t,
#                           )
#
# file = xlsx.form_sec_rep()
#
# file.save('123.xlsx')

