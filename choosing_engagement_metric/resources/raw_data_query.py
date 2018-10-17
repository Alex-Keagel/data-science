query = ''' SELECT
           |     device_id,
           |     message_type,
           |     recieved,
           |     view,
           |     time_on_page,
           |     scroll_depth
           | FROM
           |     tlv_engagement_data.flatten_mixpanel_home
           | WHERE
           |     recieved = 1 AND date >= {date_placeholder}
           |     ;'''

query_with_percentiles = '''SELECT 
                            |    temp_table.device_id,
                            |    temp_table.message_type,
                            |    temp_table.recieved,
                            |    temp_table.view,
                            |    temp_table.time_on_page,
                            |    temp_table.scroll_depth,
                            |    APPROX_PERCENTILE(temp_table.time_on_page,0.5) 
                            |       OVER (partition by temp_table.message_type) AS median_time_on_page,
                            |    APPROX_PERCENTILE(temp_table.time_on_page,0.33) 
                            |       OVER (partition by temp_table.message_type) AS first_third_time_on_page,
                            |    APPROX_PERCENTILE(temp_table.time_on_page,0.67) 
                            |       OVER (partition by temp_table.message_type) AS last_third_time_on_page,
                            |    APPROX_PERCENTILE(temp_table.scroll_depth,0.5) 
                            |       OVER (partition by temp_table.message_type) AS median_scroll_depth,
                            |    APPROX_PERCENTILE(temp_table.scroll_depth,0.33) 
                            |       OVER (partition by temp_table.message_type) AS first_third_scroll_depth,
                            |    APPROX_PERCENTILE(temp_table.scroll_depth,0.67) 
                            |       OVER (partition by temp_table.message_type) AS last_third_scroll_depth
                            |FROM
                            |(SELECT
                            |     device_id,
                            |     message_type,
                            |     recieved,
                            |     view,
                            |     case 
                            |          WHEN time_on_page>0 THEN time_on_page
                            |          ELSE 0
                            |     END AS time_on_page,
                            |     CASE 
                            |          WHEN scroll_depth>0 THEN scroll_depth
                            |          ELSE 0
                            |     END AS scroll_depth
                            | FROM
                            |     tlv_engagement_data.flatten_mixpanel_home
                            | WHERE
                            |     recieved = 1 AND date >= '{date_placeholder}'
                            |     AND view>0) AS temp_table
                            |     ;'''
