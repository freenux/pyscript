select distinct u.id, u.first_id, u.second_id, e.distinct_id, e.`$device_id`
from events e
inner join users u
on e.user_id = u.id
where e.`date` = '{start_date}' AND date <= '{end_date}'
and e.event = '$AppViewScreen'
and (u.mediasource is null OR u.mediasource = '')
and e.`$is_first_day` = 1
;

