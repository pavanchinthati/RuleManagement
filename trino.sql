CREATE MATERILIZED VIEW organization_event_counts_hourly
SELECT 
  "organization.id",
  date_trunc('hour', "alert.created") AS hour,
  COUNT(*) AS event_count
FROM 
  alert
WHERE 
  pdate = 20240822 
GROUP BY 
  "organization.id", 
  date_trunc('hour',"alert.created")
ORDER BY date_trunc('hour',"alert.created") desc;
