# Craig Tomkow
# 2022-09-09

# defines the pipeline implementation definitions such as:
#  - pipeline name (displayed to user via `pipeline --ls` command)
#          (name also used to run the pipeline via `pipeline --run NAME`)
#      - pipeline_class_name (used to extract, transform, and load)
#      - config_file_name (pipeline configuration details)
#      - config_class_name (used to parse config file)
#
definitions = {
    'pipelines': {
        'network_oncall_calendar_sync': {
            'pipeline_file_name': 'network_oncall_calendar_sync.py',
            'pipeline_directory': 'network_oncall_calendar_sync',
        }
    }
}
