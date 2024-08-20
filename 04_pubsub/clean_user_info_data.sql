

-- Use the current timestamp as a reference to filter new records
DECLARE current_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

-- Main query to filter and transform the data
INSERT INTO `PROJECT_ID.DATASET.TABLENAME_DESTINATION`
SELECT 
 
  publish_time AS created_at,
  current_timestamp() AS inserted_at,  
   message_id AS id,
  JSON_VALUE(data, '$.name') AS name,
  JSON_VALUE(data, '$.address') AS address,
  JSON_VALUE(data, '$.email') AS email,
  JSON_VALUE(data, '$.phone_number') AS phone_number,
  JSON_VALUE(data, '$.birthdate') AS birthdate,
  JSON_VALUE(data, '$.created_at') AS user_createdAt
FROM 
  `PROJECT_ID.DATASET.TABLENAME_SOURCE`
WHERE 
  publish_time > (
    SELECT MAX(created_at)
    FROM `PROJECT_ID.DATASET.TABLENAME_DESTINATION`
  )
  -- Filter only the records with a newer timestamp
  AND publish_time <= CURRENT_TIMESTAMP();
