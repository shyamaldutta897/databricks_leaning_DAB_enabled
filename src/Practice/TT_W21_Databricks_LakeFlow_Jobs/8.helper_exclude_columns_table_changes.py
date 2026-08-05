# Databricks notebook source
#This is a helper function that helps to exclude certain fields from a table
#Requirement is - table_changes function also produces a few metadata fields which wouldn't be a part of the target table while applying merge
#So this will create problem while performing merge.
#This function makes sure that we don't run into that problem

exclude_cols=['_change_type','_commit_version','_commit_timestamp']

def select_clean(df):
    cols_to_drop=[c for c in exclude_cols if c in df.columns]
    return df.drop(*cols_to_drop)
