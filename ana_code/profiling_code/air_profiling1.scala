// Inspect units: all with the same units -> dropped
df.select("units").distinct.show() 

// Inspect daily_obs_count: all is 1 -> dropped
df.select("daily_obs_count").distinct.show() 

// Inspect percent_complete: all is 1 -> dropped
df.select("percent_complete").distinct.show() 

// Show all disctinct state and state code
// All records are state = New York, state_code = 36 -> not useful, dropped 
df.select("state","state_code").distinct.show() 

// Inspect sites
df.select("site_name").distinct.count() // 41
df.select("site_id","site_name","site_latitude","site_longitude").distinct.count() // 41
df.select("site_id","site_name","site_latitude","site_longitude").distinct.orderBy("site_id").show(50)
// site_name has one-to-one relationship with site_id, site_latitude, and site_longitude
// keep site_name only

// Inspect counties
df.select("county").distinct.count() // 19
df.select("county_code").distinct.count() // 19
df.select("county","county_code").distinct.show() // keep county only

// Inspect cbsa
// Inspect counties
df.select("cbsa_code").distinct.count() // 9
df.select("cbsa_name").distinct.count() // 9
df.select("cbsa_code","cbsa_name").distinct.show() // keep cbsa_name only