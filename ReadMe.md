# Home Finance

## Предисловие
Этот пэт проект возник из вопроса, а куда уходят все деньги) 

## Из чего состоит
 - Postgres - база данных, где хранятся данные о банковских операций
 - Airflow - перегоняет данные в БД, следит за целостностью и оповещает в телегу, когда нужно добавить даных
 - Grafana - для визуализации

Все это упаковано в docker compose. Работает на маленьком домашнем сервере. 

## Как работает
Раз в неделю в телегу приходит оповещение, что надо пополнить данные. В опеределенную папку надо скинуть файлы, которые надо экспортнуть из личного кабинета тинькова. Потом можно смотреть в дашборд.

Еще есть пара вспомогательных скриптов. Для проверки целостности категорий запускается вручную. Дли чистки логов эирфлоу - запускается раз в месяц.

## Контакты
<a href = "mailto: a.poplaukhina@gmail.com">a.poplaukhina@gmail.com</a>