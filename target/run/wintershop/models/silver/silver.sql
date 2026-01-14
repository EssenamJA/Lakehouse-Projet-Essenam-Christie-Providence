
  
    

  create  table "wintershop_student"."wintershop_essenam"."silver__dbt_tmp"
  
  
    as
  
  (
    

select
    ip,
    event_ts,
    request,
    http_method,
    page,
    page_clean,
    status_code,
    referer,
    referer_host,
    user_agent,
    is_bot
from "wintershop_student"."wintershop_essenam"."stg_logs"
  );
  