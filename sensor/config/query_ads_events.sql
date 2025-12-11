select user_id,distinct_id,time,event,directed_status,channel,media_source,campaign,book_id,install_type
from events
where date = '2025-10-21'
and event in ('remarketingEvent', 'deeplink_direction', 'readingBehavior')
;

