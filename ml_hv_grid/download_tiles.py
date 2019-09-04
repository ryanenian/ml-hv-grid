"""
download_tiles.py

@author: developmentseed

Script to download a list of tiles with threading
Note: eventlet doesn't seem to play nice with debugging
"""
# import eventlet
# # Patch functions to work with green threads
# # Comment out this line when debugging
# eventlet.monkey_patch()
from os import path as op
from datetime import datetime as dt
#import logging
#import urllib
import mercantile
from decimal import Decimal
from mapbox import Static
from tqdm import tqdm
from random import random

# import boto3
# from botocore.exceptions import ClientError, EndpointConnectionError
# from botocore.vendored.requests.exceptions import ConnectTimeout

from config import preds_dir, download_params as down_p


overload_errors = ['<urlopen error [Errno 60] ETIMEDOUT>',
                   '<urlopen error [Errno 60] Operation timed out>',
                   '<urlopen error [Errno 2] Lookup timed out>',
                   '<urlopen error [Errno 8] Name or service not known>']

### Commented out code below should be used if tiles must be downloaded to AWS ###
# def bucket_check(bucket_name):
#     """Check if bucket is visible on S3"""
#
#     s3 = boto3.resource('s3', down_p['aws_region'])
#
#     bucket_names = [b.name for b in s3.buckets.all()]
#     if bucket_name in bucket_names:
#         print('Successfully found S3 bucket "{}"\n'.format(bucket_name))
#         return True
#
#     print('Can\'t connect to bucket "{}", double check permissions in IAM'.format(bucket_name))
#     return False
#
#
# def key_check(s3, bucket, key):
#     """Check if a key exists already. If any errors, return False"""
#     try:
#         s3.Object(bucket, key).load()
#     except ClientError as e:
#         # Key doesn't exist
#         if int(e.response['Error']['Code']) == 404:
#             return False
#         # Error when checking if key exists
#         print('Error on `key_check`: {}, {}'.format(key, e))
#         return False
#     return True
#
#
# def get_and_store_file(tile_ind, list_format=('x', 'y', 'z')):
#     """Fetch tile from internet and store image on S3"""
#
#     # Only download a certain percentage of tiles
#     if random() > down_p['download_prob']:
#         return 0
#     tile_ind = tile_ind.rstrip('\n').split()
#     #####################################
#     # Construct url to image
#     #####################################
#     # Allow for other x/y/z orders
#     ind_pos = [list_format.index(letter) for letter in ['x', 'y', 'z']]
#     ind_dict = dict(x=tile_ind[ind_pos[0]],
#                     y=tile_ind[ind_pos[1]],
#                     z=tile_ind[ind_pos[2]])
#     url = down_p['url_template'].format(**ind_dict)
#
#     ##############################################
#     # Setup S3 resources, and check if file exists
#     ##############################################
#     # Check if tile exists already
#     s3 = boto3.resource('s3')
#     key_fname = op.join(down_p['aws_dir'],
#                         '{x}-{y}-{z}.jpg'.format(**ind_dict))
#     key_exists = key_check(s3, down_p['aws_bucket_name'], key_fname)
#
#     ####################################################
#     # Upload to S3 if file doesn't exist
#     ####################################################
#     if not key_exists:
#         repeat_try = 10
#         while repeat_try:
#             try:
#                 req = urllib.request.urlopen(url)
#                 s3.meta.client.upload_fileobj(req, down_p['aws_bucket_name'], key_fname)
#                 print('Success on {}'.format(key_fname))
#                 return 200
#
#             except urllib.error.HTTPError as http_e:
#                 print('\nRecieved url error')
#                 logging.error('HTTPError: %s', http_e)
#                 return 429
#
#             except urllib.error.URLError as url_e:
#                 print('\nRecieved url error {}'.format(url_e))
#                 logging.error('URL Error: %s', url_e)
#                 if str(url_e) in overload_errors:
#                     repeat_try -= 1
#                     print('Known load error, retrying')
#                     continue
#                 else:
#                     return
#
#             except (ConnectTimeout, EndpointConnectionError) as epc_e:
#                 print('\nRecieved enpoint connection error {}'.format(epc_e))
#                 logging.error('Endpoint connection error: %s', epc_e)
#                 repeat_try -= 1
#                 print('AWS-side error, retrying')
#                 continue
#
#             except Exception as err:
#                 print('\bRecieved other error')
#                 logging.error('Other error on %s', key_fname)
#             req.close()
#         if repeat_try == 0:
#             print('Too many repeats, quitting on {}'.format(key_fname))
#             return
#     else:
#         #print('Key exists: {}'.format(key_fname))
#         return 0

def get_and_store_file(tile_ind, list_format=('x', 'y', 'z')):
    """Fetch tile from internet and store on local drive"""

    # Only download a certain percentage of tiles
    # if random() > down_p['download_prob']:
    #
    #     return 0
    tile_ind = tile_ind.rstrip('\n').split()
    #####################################
    # Construct url to image
    #####################################
    # Allow for other x/y/z orders
    ind_pos = [list_format.index(letter) for letter in ['x', 'y', 'z']]
    ind_dict = dict(x=tile_ind[ind_pos[0]],
                    y=tile_ind[ind_pos[1]],
                    z=tile_ind[ind_pos[2]])
    #url = down_p['url_template'].format(**ind_dict)

    service = Static(down_p['token'])
    coords = mercantile.ul(int(tile_ind[ind_pos[0]]), int(tile_ind[ind_pos[1]]), int(tile_ind[ind_pos[2]]))

    response = service.image('mapbox.satellite', lon=coords.lng, lat=coords.lat, z=tile_ind[ind_pos[2]], image_format='jpg', width=256, height=256)

    ##############################################
    # Setup file paths
    ##############################################
    # Check if tile exists already
    tile_fname = op.join(down_p['storage_dir'],
                        '{x}-{y}-{z}.jpg'.format(**ind_dict))

    ####################################################
    # Download to storage if file doesn't exist
    ####################################################
    if not op.exists(tile_fname):
        repeat_try = 10
        while repeat_try:
            try:
                print("status code", response.status_code)
                if response.status_code == 200:
                    with open(tile_fname, 'wb') as output:
                        _ = output.write(response.content)
                    print('Successfully saved', tile_fname)
                    return 200
                elif response.status_code == 429:
                    print('URL Error')
                    return 429
            except response.status_code != 200:
                repeat_try -= 1

        if repeat_try == 0:
            print('Too many repeats, quitting on {}'.format(tile_fname))
            return
    else:
        print('File exists: {}'.format(tile_fname))
        return 0


if __name__ == "__main__":

    st_dt = dt.now()


    start_time = st_dt.strftime("%m%d_%H%M%S")
    print('Start Time', str(start_time))
    #################
    # Download tiles
    #################
    # print("Creating an eventlet pool.")
    # pool = eventlet.GreenPool(size=down_p['n_green_threads'])
    tiles_downloaded = 0
    tiles_failed = 0
    ##################################
    # Open file with list of tiles
    ##################################
    # print('Iterating over file list. Using {} green threads with prob {}'.format(
    #     down_p['n_green_threads'], down_p['download_prob']))
    print('Tiles saved to: {}'.format(down_p['storage_dir']))
    with open(down_p['tile_ind_list'], 'r') as f:
        for i, l in enumerate(f):
            pass
        total = (i + 1)

    with open(down_p['tile_ind_list'], 'r') as f_tile_list:


        for ret_code in tqdm(map(get_and_store_file, f_tile_list)):

            if ret_code == 200:
                tiles_downloaded += 1
                print("succeeded", tiles_downloaded)
                print('Count', str(tiles_downloaded) + '/' + str(total))
                if tiles_downloaded % 1e3 == 0:
                    print('\nTiles saved: {:.3E}'.format(Decimal(tiles_downloaded)))

            elif ret_code == 429:

                print('Got 429 code (too many requests). Sleeping...')
            else:
                print("Failed With return code", ret_code)


            #print('Greenthreads: {} running, {} waiting, {} free'.format(
            #    pool.running(), pool.waiting(), pool.free()))

    delta = dt.now() - st_dt
    print('\nElapsed time: %s, %s per image' % (delta, delta / tiles_downloaded))

