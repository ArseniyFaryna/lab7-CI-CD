{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e13a7a39-32ff-46eb-b28c-5924fdc58da0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Apply row level security and column level security"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "4a7e9007-34af-4997-ae88-138d29c07ec0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%python\n",
    "table_path = \"dbr_dev.faryna_gold.agg_weather_weekly\"\n",
    "\n",
    "df = spark.read.table(table_path)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE FUNCTION dbr_dev.faryna_gold.mask_load_date(load_date DATE)\n",
    "RETURNS DATE\n",
    "RETURN CASE\n",
    "  WHEN current_user() = 'farynaarseniy2007@softserve.academy' THEN load_date \n",
    "  ELSE DATE('1900-01-01') \n",
    "END;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "%sql\n",
    "ALTER MATERIALIZED VIEW dbr_dev.faryna_gold.agg_weather_weekly\n",
    "ALTER COLUMN week_start SET MASK dbr_dev.faryna_gold.mask_load_date;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE FUNCTION dbr_dev.faryna_gold.weather_row_filter(load_date DATE)\n",
    "RETURNS BOOLEAN\n",
    "RETURN load_date >= '2026-03-15';"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE VIEW dbr_dev.faryna_gold.weather_secure_view AS\n",
    "SELECT *\n",
    "FROM dbr_dev.faryna_gold.agg_weather_weekly\n",
    "WHERE week_start IS NOT NULL \n",
    "  AND week_start >= '2026-03-15';"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 7020922835377300,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "Apply_rls_and_cls",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
