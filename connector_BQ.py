import os
import json
import requests
import pandas as pd
from google.cloud import bigquery

class BQ_Connector:

    def load_data(client, dataset_name, table_name, data):
        """ Load data of type DataFrame to BigQuery Table
        
        Args:
            client (client): BigQuery client
            dataset_name (str): Dataset Name in BigQuery
            table_name (str): Table Name in BigQuery
            data (DataFrame): DataFrame to be inserted to a table in BigQuery
        """
        dataset = client.dataset(dataset_name)
        table_id = dataset.table(table_name)

        job_config = bigquery.LoadJobConfig()
        load_job = client.load_table_from_dataframe(data, table_id, job_config = job_config)
        return load_job.result()


    def delete_data(client, dataset_name, table_name):
        """ Empty a table in BigQuery
        
        Args:
            client (client): BigQuery client
            dataset_name (str): Dataset Name in BigQuery
            table_name (str): Table Name in BigQuery
        """

        delete_statement = (
        "TRUNCATE TABLE " + dataset_name + "." + table_name + " "
        )
        query_job = client.query(delete_statement)
        query_job.result()
        return True
        
        
    def delete_when_match(client, dataset_name_ori, table_name_ori,dataset_name_temp,table_name_temp,condition):
        """ Delete rows in main table when it is already existed

        Args:
            client (client): BigQuery client
            dataset_name_ori (str): Main Dataset Name in BigQuery
            table_name_ori (str): Main Table Name in BigQuery
            dataset_name_temp (str): Dataset Name in BigQuery for Temporary Storage
            table_name_temp (str): Table Name in BigQuery for Temporary Storage
            condition (str): "WHERE" condition to delete when it matches original columns
        """

        delete_statement = (
        "MERGE "+ dataset_name_ori + "." + table_name_ori + " ori "
        "USING "+ dataset_name_temp + "." + table_name_temp + " temp "
        + condition +
        "WHEN MATCHED THEN DELETE"
        )
        query_job = client.query(delete_statement)
        query_job.result()
        return True
    
    def get_recent_date(client, date_column, dataset_name, table_name):
        """ Get the most Recent Update Date of Table in BigQuery
        
        Args:
            client (client): BigQuery client
            date_column (str): Column that contains "DATE"
            dataset_name (str): Dataset Name in BigQuery
            table_name (str): Table Name in BigQuery
            
        Return:
            str: The most recent date of data from targeted table
        """
        
        query = f"""SELECT {date_column} FROM `ydmdashboard.{dataset_name}.{table_name}` 
                ORDER BY {date_column} desc 
                LIMIT 1"""
        result = client.query(query).result()
        rows = list(result)
        recent_date = rows[0][date_column].strftime('%Y-%m-%d')

        return recent_date
    
    def create_view_unpivot(client,dataset_name,table_name,view_name,action_type,pivot_action_type):
        """ Create unpivot View from table in BigQuery
        
        Args:
            client (client): BigQuery client
            dataset_name (str): Dataset Name in BigQuery
            table_name (str): Table Name in BigQuery
            view_name (str): View Name in BigQuery
            action_type (str): Column Names to be use in Destination View
            pivot_action_type (str): Column Names to be use in Destination View
        """
        # Fetch distinct action types
        distinct_action_types_query = f"""
        SELECT DISTINCT {action_type}
        FROM `ydmdashboard.{dataset_name}.{table_name}`
        """
        distinct_action_types_result = client.query(distinct_action_types_query)
        distinct_action_types = [row.action_type for row in distinct_action_types_result]

        # Constructing the SQL query dynamically
        pivot_query = f"""
        SELECT *
        FROM `ydmdashboard.{dataset_name}.{table_name}`
        PIVOT (
        SUM(value) FOR {pivot_action_type} IN ({", ".join([f"'{action_type}'" for action_type in distinct_action_types])})
        );
        """

        # Creating a view with the dynamically generated pivot query
        view_name = f"{dataset_name}.{view_name}"
        view_query = f"CREATE OR REPLACE VIEW `{view_name}` AS {pivot_query}"
        client.query_and_wait(view_query)
        return True
    
    def get_row_to_add(client,project_id, dataset_id, action_table,main_table):
        """ Get Row of ad_id and date_start that don't exist in the main table
            and insert empty row with missing ad_id and date_start
        
        Args:
            client (client): BigQuery client
            project_id (str): Project ID in BigQuery
            dataset_id (str): Dataset Name in BigQuery
            action_table (str): Action Table Name in BigQuery
            main_table (str): Main Table Name in BigQuery
        
        Returns:
            List: A list of rows to be inserted to BigQuery main table
        """
     
        source_query = f"""
        SELECT DISTINCT
        account_id, campaign_id, adset_id, ad_id, date_start
        FROM `{project_id}.{dataset_id}.{action_table}`
        """

        target_query = f"""
        SELECT DISTINCT
        account_id, campaign_id, adset_id, ad_id,date_start
        FROM `{project_id}.{dataset_id}.{main_table}`
        """
        
        source_df = client.query(source_query).to_dataframe()
        target_df = client.query(target_query).to_dataframe()
        
        new_rows = source_df.merge(target_df, how='left', indicator=True)
        new_rows = new_rows[new_rows['_merge'] == 'left_only'].drop('_merge', axis=1)
        
        return new_rows

    def query_table(table_name):
        client = bigquery.Client()
        query = f"SELECT * FROM `{table_name}`"
        query_job = client.query(query)
        rows = query_job.result()
        return pd.DataFrame([dict(row) for row in rows])
    
    def query_table_date(table_name,target_date):
        client = bigquery.Client()
        query = f"SELECT * FROM `{table_name}` WHERE post_time_human >= '{target_date}'"
        query_job = client.query(query)
        rows = query_job.result()
        return pd.DataFrame([dict(row) for row in rows])

    def pivot_to_nested(df, id_cols, column_name):
        """ Convert all columns except id_cols into a nested key-value structure
        
        Args:
            df (DataFrame): target DataFrame
            id_cols (list[str]): Column Name used in pivot
        
        Returns:
            DataFrame: A DataFrame with id_cols + nested column
        """

        # Melt DataFrame to create key-value pairs
        melted_df = df.melt(id_vars=id_cols, var_name="key", value_name="value").dropna(subset=["value"])
        
        # Group by id_cols and aggregate as a nested structure
        nested_df = melted_df.groupby(id_cols).apply(
            lambda group: group[["key", "value"]].to_dict("records")
        ).reset_index(name=column_name)
        
        return nested_df