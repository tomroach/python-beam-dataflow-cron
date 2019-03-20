import apache_beam as beam
import json
import urllib


def run():

    names_key = { '_comments'      : 'comments' ,
              '_direction'         : 'direction' ,
              '_fromst'            : 'from_street', 
              '_last_updt'         : 'last_updated',
              '_length'            : 'length',
              '_lif_lat'           : 'start_lat',
              '_lit_lat'           : 'end_lat',
              '_lit_lon'           : 'end_lon',
              '_strheading'        : 'street_heading' ,
              '_tost'              : 'to_street' ,
              '_traffic'           : 'traffic_speed',
              'segmentid'          : 'segment_id', 
              'start_lon'          : 'start_lon',
              'street'             : 'street'
              }

    schema = "segment_id:INT64,traffic_speed:INT64,street:STRING,street_heading:STRING,direction:STRING,from_street:STRING,to_street:STRING,last_updated:DATETIME,length:STRING,start_lon:FLOAT64,start_lat:FLOAT64,end_lon:FLOAT64,end_lat:FLOAT64,comments:STRING"

    url = "https://data.cityofchicago.org/resource/n4j6-wkkf.json?$limit=5000"

    response = urllib.urlopen(url)

    records = json.loads(response.read())

    for row in records:
      for k, v in names_key.items():
        for old_name in row:
          if k == old_name:
            row[v] = row.pop(old_name)

    PROJECT = "festive-tiger-233500"
    DATASET = "traffic"
    TABLE = "chicago_traffic_segment_current"

    with beam.Pipeline(argv=["--project", PROJECT]) as p:

        records_to_upload = p | beam.Create(records)

        records_to_upload | 'Write' >> beam.io.WriteToBigQuery(
                ("%s:%s.%s" % (PROJECT, DATASET, TABLE)),
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

        p.run()


if __name__ == "__main__":
    run()

