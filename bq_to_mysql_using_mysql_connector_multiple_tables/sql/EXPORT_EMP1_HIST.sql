EXPORT DATA OPTIONS
(
	uri='gs://{{ params.bucket_name }}/{{ params.folder_name }}/{{ params.file_name }}_*.csv',
	format='CSV',
	 header = false,
	overwrite=true,
	field_delimiter=','
) AS

   SELECT * FROM {{ params.project_name }}.DB_CE_TEMP.EMP1;