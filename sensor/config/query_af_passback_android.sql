select time,distinct_id,u.googleAdId,u.second_id,u.first_id,passback_content 
from events e inner join users u
on e.user_id = u.id
where 
event = 'af_passback' AND
e.passback_content like '%"Non-organic"%' AND
date >= '{start_date}' and date <= '{end_date}'
;
