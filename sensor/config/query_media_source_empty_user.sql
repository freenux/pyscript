select u.first_id, u.second_id, u.id, u.Campaign,
e.distinct_id, e.device_id, e.`$device_id`, e.deviceId, e.`$is_first_day`,
e.advertiseing_id, e.advertising_id, e.appsflyer_id, e.af_app_id
from events e 
inner join users u
on e.user_id = u.id
where e.date >= '{start_date}' and e.date <= '{end_date}'
and e.`$is_first_day` = 1
and e.event = '$AppViewScreen'
and (u.mediasource is null or u.mediasource = '')
and e.channel = 'dreameapp-23';
