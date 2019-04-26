import datetime
import os
import glob
import copy

from astropy import table
from astropy import time
from celery.utils.log import get_task_logger
import ephem
import gwemopt.utils
import gwemopt.moc
import gwemopt.gracedb
import gwemopt.rankedTilesGenerator
import gwemopt.waw
import gwemopt.lightcurve
import gwemopt.coverage
import gwemopt.efficiency
import gwemopt.plotting
import gwemopt.tiles
import gwemopt.segments
import gwemopt.catalog
from ligo import segments
import numpy as np

import growth
from . import celery
from .. import models

log = get_task_logger(__name__)

__all__ = ('tile',)


def params_struct(dateobs, tobs=None, filt=['r'], exposuretimes=[60.0],
                  mindiff=30.0*60.0, probability=0.9, tele='ZTF',
                  airmass=2.5,
                  schedule_type='greedy',
                  doReferences=True,
                  filterScheduleType='block',
                  schedule_strategy='tiling'):

    growthpath = os.path.dirname(growth.__file__)
    config_directory = os.path.join(growthpath, 'too', 'config')
    tiling_directory = os.path.join(growthpath, 'too', 'tiling')
    catalog_directory = os.path.join(growthpath, 'too', 'catalog')

    params = {}
    params["config"] = {}
    config_files = glob.glob("%s/*.config" % config_directory)
    for config_file in config_files:
        telescope = config_file.split("/")[-1].replace(".config", "")
        params["config"][telescope] =\
            gwemopt.utils.readParamsFromFile(config_file)
        params["config"][telescope]["telescope"] = telescope
        if "tesselationFile" in params["config"][telescope]:
            params["config"][telescope]["tesselationFile"] =\
                os.path.join(config_directory,
                             params["config"][telescope]["tesselationFile"])
            tesselation_file = params["config"][telescope]["tesselationFile"]
            if not os.path.isfile(tesselation_file):
                if params["config"][telescope]["FOV_type"] == "circle":
                    gwemopt.tiles.tesselation_spiral(
                        params["config"][telescope])
                elif params["config"][telescope]["FOV_type"] == "square":
                    gwemopt.tiles.tesselation_packing(
                        params["config"][telescope])

            params["config"][telescope]["tesselation"] =\
                np.loadtxt(params["config"][telescope]["tesselationFile"],
                           usecols=(0, 1, 2), comments='%')

        if "referenceFile" in params["config"][telescope]:
            params["config"][telescope]["referenceFile"] =\
                os.path.join(config_directory,
                             params["config"][telescope]["referenceFile"])
            refs = table.unique(table.Table.read(
                params["config"][telescope]["referenceFile"],
                format='ascii', data_start=2, data_end=-1)['field', 'fid'])
            reference_images =\
                {group[0]['field']: group['fid'].astype(int).tolist()
                 for group in refs.group_by('field').groups}
            reference_images_map = {1: 'g', 2: 'r', 3: 'i', 4: 'z', 5: 'J'}
            for key in reference_images:
                reference_images[key] = [reference_images_map.get(n, n)
                                         for n in reference_images[key]]
            params["config"][telescope]["reference_images"] = reference_images

        observer = ephem.Observer()
        observer.lat = str(params["config"][telescope]["latitude"])
        observer.lon = str(params["config"][telescope]["longitude"])
        observer.horizon = str(-12.0)
        observer.elevation = params["config"][telescope]["elevation"]
        params["config"][telescope]["observer"] = observer

    params["skymap"] = ""
    params["gpstime"] = -1
    params["outputDir"] = "output/%s" % dateobs.strftime("%Y%m%dT%H%M%S")
    params["tilingDir"] = tiling_directory
    params["event"] = ""
    params["telescopes"] = [tele]
    if schedule_strategy == "catalog":
        params["tilesType"] = "galaxy"
        params["catalogDir"] = catalog_directory
        params["galaxy_catalog"] = "CLU"
        params["galaxy_grade"] = "S"
        params["writeCatalog"] = False
        params["catalog_n"] = 1.0
        params["powerlaw_dist_exp"] = 1.0
        params["doChipGaps"] = False
    elif schedule_strategy == "tiling":
        params["tilesType"] = "moc"
        if tele == "ZTF":
            params["doChipGaps"] = True
        else:
            params["doChipGaps"] = False
    params["scheduleType"] = schedule_type
    params["timeallocationType"] = "powerlaw"
    params["nside"] = 512
    params["powerlaw_cl"] = probability
    params["powerlaw_n"] = 1.0
    params["powerlaw_dist_exp"] = 0.0

    params["doPlots"] = False
    params["doMovie"] = False
    params["doObservability"] = True
    params["do3D"] = False

    params["doFootprint"] = False
    params["footprint_ra"] = 30.0
    params["footprint_dec"] = 60.0
    params["footprint_radius"] = 10.0

    params["airmass"] = airmass

    params["doCommitDatabase"] = True
    params["doRequestScheduler"] = False
    params["dateobs"] = dateobs
    params["doEvent"] = False
    params["doSkymap"] = False
    params["doFootprint"] = False
    params["doDatabase"] = True
    params["doReferences"] = doReferences
    params["doSplit"] = False
    params["doParallel"] = False
    params["doUseCatalog"] = False

    if tele in ["KPED", "GROWTH-India"]:
        params["doMinimalTiling"] = True
    else:
        params["doMinimalTiling"] = False
    params["doIterativeTiling"] = False
    params["galaxies_FoV_sep"] = 1.0

    if params["doEvent"]:
        params["skymap"], eventinfo = gwemopt.gracedb.get_event(params)
        params["gpstime"] = eventinfo["gpstime"]
        event_time = time.Time(params["gpstime"], format='gps', scale='utc')
        params["dateobs"] = event_time.iso
    elif params["doSkymap"]:
        event_time = time.Time(params["gpstime"], format='gps', scale='utc')
        params["dateobs"] = event_time.iso
    elif params["doFootprint"]:
        params["skymap"] = gwemopt.footprint.get_skymap(params)
        event_time = time.Time(params["gpstime"], format='gps', scale='utc')
        params["dateobs"] = event_time.iso
    elif params["doDatabase"]:
        event_time = time.Time(params["dateobs"], format='datetime',
                               scale='utc')
        params["gpstime"] = event_time.gps
    else:
        raise ValueError('Need to enable --doEvent, --doFootprint, '
                         '--doSkymap, or --doDatabase')

    if tobs is None:
        now_time = time.Time.now()
        timediff = now_time.gps - event_time.gps
        timediff_days = timediff / 86400.0
        params["Tobs"] = np.array([timediff_days, timediff_days+1])
    else:
        params["Tobs"] = tobs

    params["doSingleExposure"] = True
    if filterScheduleType == "block":
        params["doAlternatingFilters"] = True
    else:
        params["doAlternatingFilters"] = False
    params["filters"] = filt
    params["exposuretimes"] = exposuretimes
    params["mindiff"] = mindiff

    params = gwemopt.segments.get_telescope_segments(params)

    if params["doPlots"]:
        if not os.path.isdir(params["outputDir"]):
            os.makedirs(params["outputDir"])

    return params


def gen_structs(params):

    log.info('Loading skymap')
    # Function to read maps
    map_struct = gwemopt.utils.read_skymap(params, is3D=params["do3D"],
                                           map_struct=params['map_struct'])

    if params["tilesType"] == "galaxy":
        print("Generating catalog...")
        map_struct, catalog_struct =\
            gwemopt.catalog.get_catalog(params, map_struct)

    if params["tilesType"] == "moc":
        log.info('Generating MOC struct')
        moc_structs = gwemopt.moc.create_moc(params, map_struct=map_struct)
        tile_structs = gwemopt.tiles.moc(params, map_struct, moc_structs)
    elif params["tilesType"] == "ranked":
        log.info('Generating ranked struct')
        moc_structs = gwemopt.rankedTilesGenerator.create_ranked(params,
                                                                 map_struct)
        tile_structs = gwemopt.tiles.moc(params, map_struct, moc_structs)
    elif params["tilesType"] == "hierarchical":
        log.info('Generating hierarchical struct')
        tile_structs = gwemopt.tiles.hierarchical(params, map_struct)
    elif params["tilesType"] == "greedy":
        log.info('Generating greedy struct')
        tile_structs = gwemopt.tiles.greedy(params, map_struct)
    elif params["tilesType"] == "galaxy":
        log.info("Generating galaxy struct...")
        tile_structs = gwemopt.tiles.galaxy(params, map_struct, catalog_struct)
    else:
        raise ValueError(
            'Need tilesType to be moc, greedy, hierarchical, galaxy or ranked')

    coverage_struct = gwemopt.coverage.timeallocation(params,
                                                      map_struct,
                                                      tile_structs)

    if params["doPlots"]:
        gwemopt.plotting.skymap(params, map_struct)
        gwemopt.plotting.tiles(params, map_struct, tile_structs)
        gwemopt.plotting.coverage(params, map_struct, coverage_struct)

    return map_struct, tile_structs, coverage_struct


def get_planned_observations(
        params, map_struct, tile_structs, coverage_struct):

    nside = map_struct["nside"]

    if params["doCommitDatabase"]:
        telescope = params["telescopes"][0]
        config_struct = params["config"][telescope]

        field_ids, ras, decs, probs, nexposures = [], [], [], [], []
        segmentlist = segments.segmentlist()
        totprob = 0.0
        for field_id in tile_structs[telescope].keys():
            tile_struct = tile_structs[telescope][field_id]
            ra, dec = tile_struct["ra"], tile_struct["dec"]

            if tile_struct["nexposures"] > 0.0:
                field_ids.append(field_id)
                ras.append(tile_struct["ra"])
                decs.append(tile_struct["dec"])
                probs.append(tile_struct["prob"])
                nexposures.append(tile_struct["nexposures"])
                totprob = totprob+tile_struct["prob"]

                segmentlist = segmentlist + tile_struct["segmentlist"]
                segmentlist = segmentlist.coalesce()

                if params["tilesType"] == "galaxy":
                    ref_filter_mags, ref_filter_bands = [], []
                    ref_filter_ids = []

                    if config_struct["FOV_type"] == "square":
                        ipix, radecs, patch, area =\
                            gwemopt.utils.getSquarePixels(ra, dec,
                                                          config_struct["FOV"],
                                                          nside)
                    elif config_struct["FOV_type"] == "circle":
                        ipix, radecs, patch, area =\
                            gwemopt.utils.getCirclePixels(ra, dec,
                                                          config_struct["FOV"],
                                                          nside)
                    if len(radecs) == 0:
                        continue
                    corners = np.vstack((radecs, radecs[0, :]))
                    if corners.size == 10:
                        corners_copy = copy.deepcopy(corners)
                        corners[2] = corners_copy[3]
                        corners[3] = corners_copy[2]

                    contour = {
                        'type': 'Feature',
                        'geometry': {
                            'type': 'MultiLineString',
                            'coordinates': [corners.tolist()]
                        },
                        'properties': {
                            'field_key': "%s_%d" % (telescope, int(field_id)),
                            'telescope': telescope,
                            'field_id': int(field_id),
                            'ra': ra,
                            'dec': dec,
                            'reference_filter_ids': ref_filter_ids,
                            'reference_filter_bands': ref_filter_bands,
                            'reference_filter_mags': ref_filter_mags
                        }
                    }
                    field = models.Field(telescope=telescope,
                                         field_id=int(field_id),
                                         ra=ra, dec=dec,
                                         contour=contour,
                                         reference_filter_ids=ref_filter_ids,
                                         reference_filter_mags=ref_filter_mags,
                                         ipix=ipix.tolist())
                    models.db.session.merge(field)

        filter_ids = {"g": 1, "r": 2, "i": 3, "z": 4, "J": 5}
        for ii in range(len(coverage_struct["ipix"])):
            data = coverage_struct["data"][ii, :]
            filt = coverage_struct["filters"][ii]
            filter_id = filter_ids[filt]
            mjd = data[2]
            tt = time.Time(mjd, format='mjd')

            if config_struct["overhead_per_exposure"] is not None:
                overhead_per_exposure = config_struct["overhead_per_exposure"]
            else:
                overhead_per_exposure = 0.0

            exposure_time, field_id, prob = data[4], data[5], data[6]

            yield models.PlannedObservation(
                obstime=tt.datetime,
                field_id=field_id,
                exposure_time=exposure_time,
                weight=prob,
                filter_id=filter_id,
                telescope=telescope,
                planned_observation_id=ii,
                overhead_per_exposure=overhead_per_exposure)


@celery.task(ignore_result=True, shared=False)
def tile(localization_name, dateobs, telescope,
         validity_window_start=None,
         validity_window_end=None,
         plan_name=None,
         **plan_args):

    if validity_window_start is None:
        validity_window_start = datetime.datetime.now()
    if validity_window_end is None:
        validity_window_end = validity_window_start + datetime.timedelta(1)

    plan_args = dict(plan_args)
    plan_args.setdefault('tobs', [
        time.Time(validity_window_start).mjd - time.Time(dateobs).mjd,
        time.Time(validity_window_end).mjd - time.Time(dateobs).mjd])

    exposuretimes = plan_args['exposuretimes']
    if plan_args['doDither'] and telescope == 'DECam':
        # Add dithering
        exposuretimes = [2*x for x in exposuretimes]

    plan_args.setdefault('probability', 0.9)

    if plan_name is None:
        plan_name = "%s_%s_%d_%d_%s_%d_%d" % (
            "".join(plan_args['filt']), plan_args['schedule_type'],
            plan_args['doDither'], plan_args['doReferences'],
            plan_args['filterScheduleType'],
            exposuretimes[0],
            100 * plan_args['probability'])

    localization = models.Localization.query.filter_by(
        dateobs=dateobs, localization_name=localization_name).one()

    plan = models.Plan(dateobs=dateobs,
                       plan_name=plan_name,
                       telescope=telescope,
                       validity_window_start=validity_window_start,
                       validity_window_end=validity_window_end,
                       plan_args=plan_args)
    models.db.session.merge(plan)
    models.db.session.commit()

    params = params_struct(dateobs, tobs=np.asarray(plan_args['tobs']),
                           filt=plan_args['filt'],
                           exposuretimes=exposuretimes,
                           probability=plan_args['probability'],
                           tele=telescope,
                           schedule_type=plan_args['schedule_type'],
                           doReferences=plan_args['doReferences'],
                           filterScheduleType=plan_args['filterScheduleType'],
                           schedule_strategy=plan_args['schedule_strategy'])

    params['map_struct'] = dict(
        zip(['prob', 'distmu', 'distsigma', 'distnorm'], localization.flat))

    params['is3D'] = localization.is_3d
    params['localization_name'] = localization_name
    map_struct, tile_structs, coverage_struct = gen_structs(params)

    for planned_observation in get_planned_observations(
            params, map_struct, tile_structs, coverage_struct):
        plan.planned_observations.append(planned_observation)
    plan.status = plan.Status.READY
    models.db.session.merge(plan)
    models.db.session.commit()
