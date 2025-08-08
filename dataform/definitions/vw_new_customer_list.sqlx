config {
  type: "view",
  schema: "silver", 
  name: "vw_new_customer_list",
  description: "Vista NEW CUSTOMER LIST",
  tags: ["silver", "ltm", "new_customer_list"]
}

SELECT c.id AS `Customer ID`                                                   
     , c.name                    AS `Customer Name` 
     , DATE(c.created_on)        AS `Created On` 
     , c.type                    AS `Type` 
     , camp.name                 AS `Original Campaign` 
     , DATE(MAX(j.completed_on)) AS `Last Job Completed` 
     , COUNT(j.id)               AS `Jobs Qty`                                 
     , DATE(camp.created_on)     AS campaign_created_date 
  FROM `${dataform.projectId}.${dataform.vars.raw_dataset}.customer` c 
  LEFT JOIN `${dataform.projectId}.${dataform.vars.raw_dataset}.job` j 
    ON j.customer_id             = c.id 
   AND j.job_status              = 'Completed' 
  LEFT JOIN (                                                    
        SELECT j2.campaign_id 
             , j2.customer_id 
             , c2.name
             , c2.created_on
             , ROW_NUMBER() OVER (PARTITION BY j2.customer_id ORDER BY j2.created_on ASC) as rn
          FROM `${dataform.projectId}.${dataform.vars.raw_dataset}.job`       j2
          JOIN `${dataform.projectId}.${dataform.vars.raw_dataset}.campaigns` c2
            ON j2.campaign_id    = c2.id
         WHERE j2.campaign_id    IS NOT NULL
        ) camp
    ON camp.customer_id          = c.id                                       
   AND camp.rn                   = 1                                                         
 GROUP BY c.id
     , c.name
     , c.created_on
     , c.type
     , camp.name
     , camp.created_on