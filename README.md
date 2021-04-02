# ibea_to_pg

<a href="https://github.com/alright0/ibea_to_pg/blob/main/ibea_to_pg.py">Скрипт</a> реализует возможность собирать информацию с камер IBEA и записывать в Postgres. Камеры IBEA - это скоростные камеры, созданные для контроля качества продукции, выпускаемой на производственных линиях по производству жестебанки. 

Во время работы камера обновляет локальный csv файл статистики, доступный во внутренней сети по адресу "\\xx.xx.xx.xx\statistics\actual.csv". Скрипт опрашивает камеры, запуская для каждого из указанных адресов свой тред, который, 1 раз в указанный промежуток времени(60 сек), опрашивает свой адрес. 
Если камера по адресу недоступна, скрипт пишет в лог соответствующее сообщение. Каждую успешную запись пишет в stdout:

Структура лога:<br>
<p align="center"><img width=700px src="https://user-images.githubusercontent.com/71926912/113443289-57dadb80-93fa-11eb-8fad-2812e1024191.PNG"></p>

Вывод:<br>
<p align="center"><img width=700px src="https://user-images.githubusercontent.com/71926912/113443505-ca4bbb80-93fa-11eb-955a-bc23c7185e15.PNG"></p>

Структура таблицы в базе:<br>
<p align="center"><img width=700px src="https://user-images.githubusercontent.com/71926912/113443619-04b55880-93fb-11eb-8a49-95712530bbe1.PNG"></p>

В данный момент есть excel файл для быстрого анализа ситуации в производстве и адекватности работы скрипта, получающий таблицу через ODBC(выработка линии и брак на основании выброса):<br>
<p align="center"><img width=700px src="https://user-images.githubusercontent.com/71926912/113445081-d2f1c100-93fd-11eb-9124-4291ca84c340.PNG"></p>

данные, получаемые из этой таблицы активно используются при работе с <a href="https://github.com/alright0/intranet_api">дэшбордом</a>.

Также в Postgres были настроены уведомления(notify), отправляющие JSON с новыми записями. Вывод <a href="https://github.com/alright0/ibea_to_pg/blob/main/listen_test.py">скрипта</a> для тестирования этого функционала выглядит следующим образом:<br>
<p align="center"><img width=700px src="https://user-images.githubusercontent.com/71926912/113445698-113bb000-93ff-11eb-8093-66ac1a7f2ded.PNG"></p>

В последствии его функциональность должна помочь снизить нагрузку на базу(в случаях, когда нужно получать последние измерения на камере, но не опрашивать таблицу регулярно) 

<!-- <p align="center"><img width=700px src=""></p> -->
