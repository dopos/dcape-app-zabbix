/*
 Отключить алертинг (при запуске копии на продовых данных)

 Автор: Пазычев
*/

update media_type set status =3;
update actions set status = 1;
