import logging
from urllib.parse import urlparse

from astropy import time
from celery import group
from flask import render_template
import gcn
import scipy.stats

from .flask import app
from . import models
from . import tasks

__all__ = ('listen,')

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def get_dateobs(root):
    """Get the UTC event time from a GCN notice, rounded to the nearest second,
    as a datetime.datetime object."""
    dateobs = time.Time(root.find("./WhereWhen/{*}ObsDataLocation"
                                  "/{*}ObservationLocation"
                                  "/{*}AstroCoords"
                                  "[@coord_system_id='UTC-FK5-GEO']"
                                  "/Time/TimeInstant/ISOTime").text,
                        precision=0)

    # FIXME: https://github.com/astropy/astropy/issues/7179
    dateobs = time.Time(dateobs.iso)

    return dateobs.datetime


def get_tags(root):
    """Get source classification tag strings from GCN notice."""
    # Get event stream.
    mission = urlparse(root.attrib['ivorn']).path.lstrip('/')
    yield mission

    # What type of burst is this: GRB or GW?
    try:
        value = root.find("./Why/Inference/Concept").text
    except AttributeError:
        pass
    else:
        if value == 'process.variation.burst;em.gamma':
            yield 'GRB'
        elif value == 'process.variation.trans;em.gamma':
            yield 'transient'

    # LIGO/Virgo alerts don't provide the Why/Inference/Concept tag,
    # so let's just identify it as a GW event based on the notice type.
    notice_type = gcn.get_notice_type(root)
    if notice_type in {gcn.NoticeType.LVC_PRELIMINARY,
                       gcn.NoticeType.LVC_INITIAL,
                       gcn.NoticeType.LVC_UPDATE,
                       gcn.NoticeType.LVC_RETRACTION}:
        yield 'GW'

    # Is this a retracted LIGO/Virgo event?
    if notice_type == gcn.NoticeType.LVC_RETRACTION:
        yield 'retracted'

    # Is this a short GRB, or a long GRB?
    try:
        value = root.find(".//Param[@name='Long_short']").attrib['value']
    except AttributeError:
        pass
    else:
        if value != 'unknown':
            yield value.lower()

    # Gaaaaaah! Alerts of type FERMI_GBM_SUBTHRESH store the
    # classification in a different property!
    try:
        value = root.find(
            ".//Param[@name='Duration_class']").attrib['value'].title()
    except AttributeError:
        pass
    else:
        if value != 'unknown':
            yield value.lower()

    # Get LIGO/Virgo source classification, if present.
    classifications = [
        (float(elem.attrib['value']), elem.attrib['name']) for elem in
        root.iterfind("./What/Group[@type='Classification']/Param")]
    if classifications:
        _, classification = max(classifications)
        yield classification

    search = root.find("./What/Param[@name='Search']")
    if search is not None:
        yield search.attrib['value']


def get_skymap(gcn_notice, root):
    mission = urlparse(root.attrib['ivorn']).path.lstrip('/')

    # Try Fermi GBM convention
    if gcn_notice.notice_type == gcn.NoticeType.FERMI_GBM_FIN_POS:
        url = root.find(
            "./What/Param[@name='LocationMap_URL']").attrib['value']
        url = url.replace('http://', 'https://')
        url = url.replace('_locplot_', '_healpix_')
        url = url.replace('.png', '.fit')
        return tasks.skymaps.download.s(url, gcn_notice.dateobs)

    # Try Fermi GBM **subthreshold** convention. Stupid, stupid, stupid!!
    if gcn_notice.notice_type == gcn.NoticeType.FERMI_GBM_SUBTHRESH:
        url = root.find("./What/Param[@name='HealPix_URL']").attrib['value']
        return tasks.skymaps.download.s(url, gcn_notice.dateobs)

    # Try LVC convention
    skymap = root.find("./What/Group[@type='GW_SKYMAP']")
    if skymap is not None:
        children = skymap.getchildren()
        for child in children:
            if child.attrib['name'] == 'skymap_fits':
                url = child.attrib['value']
                break

        return tasks.skymaps.download.s(url, gcn_notice.dateobs)

    retraction = root.find("./What/Param[@name='Retraction']")
    if retraction is not None:
        retraction = int(retraction.attrib['value'])
        if retraction == 1:
            return None

    # Try error cone
    loc = root.find('./WhereWhen/ObsDataLocation/ObservationLocation')
    if loc is None:
        return None

    ra = loc.find('./AstroCoords/Position2D/Value2/C1')
    dec = loc.find('./AstroCoords/Position2D/Value2/C2')
    error = loc.find('./AstroCoords/Position2D/Error2Radius')

    if None in (ra, dec, error):
        return None

    ra = float(ra.text)
    dec = float(dec.text)
    error = float(error.text)

    # Apparently, all experiments *except* AMON report a 1-sigma error radius.
    # AMON reports a 90% radius, so for AMON, we have to convert.
    if mission != 'AMON':
        error /= scipy.stats.chi(df=2).ppf(0.95)

    return tasks.skymaps.from_cone.s(ra, dec, error, gcn_notice.dateobs)


# Only produce voice/SMS alerts for events that have these tags
DESIRABLE_TAGS = {'short', 'GW', 'AMON'}
# Ignore certain tages
UNDESIRABLE_TAGS = {'transient', 'MDC', 'retracted'}


@gcn.include_notice_types(
    gcn.NoticeType.FERMI_GBM_FLT_POS,
    gcn.NoticeType.FERMI_GBM_GND_POS,
    gcn.NoticeType.FERMI_GBM_FIN_POS,
    gcn.NoticeType.FERMI_GBM_SUBTHRESH,
    gcn.NoticeType.LVC_PRELIMINARY,
    gcn.NoticeType.LVC_INITIAL,
    gcn.NoticeType.LVC_UPDATE,
    gcn.NoticeType.LVC_RETRACTION,
    gcn.NoticeType.AMON_ICECUBE_COINC,
    gcn.NoticeType.AMON_ICECUBE_HESE,
    gcn.NoticeType.ICECUBE_ASTROTRACK_GOLD,
    gcn.NoticeType.ICECUBE_ASTROTRACK_BRONZE
)
def handle(payload, root):
    with app.app_context():
        dateobs = get_dateobs(root)

        event = models.db.session.merge(models.Event(dateobs=dateobs))
        old_tags = set(event.tags)
        tags = [
            models.Tag(dateobs=event.dateobs, text=_) for _ in get_tags(root)]

        gcn_notice = models.GcnNotice(
            content=payload,
            ivorn=root.attrib['ivorn'],
            notice_type=gcn.get_notice_type(root),
            stream=urlparse(root.attrib['ivorn']).path.lstrip('/'),
            date=root.find('./Who/Date').text,
            dateobs=event.dateobs)

        for tag in tags:
            models.db.session.merge(tag)
        models.db.session.merge(gcn_notice)
        models.db.session.commit()
        new_tags = set(event.tags)

        skymap = get_skymap(gcn_notice, root)
        if skymap is not None:
            (
                skymap | group(
                    *(
                        tasks.tiles.tile.s(
                            dateobs, tele.telescope, **tele.default_plan_args
                        )
                        for tele in models.Telescope.query
                    ),
                    tasks.skymaps.contour.s(dateobs)
                )
            ).delay()

        old_alertable = bool((DESIRABLE_TAGS & old_tags) and not
                             (UNDESIRABLE_TAGS & old_tags))
        new_alertable = bool((DESIRABLE_TAGS & new_tags) and not
                             (UNDESIRABLE_TAGS & new_tags))
        if old_alertable != new_alertable:
            tasks.twilio.call_everyone.delay(
                'event_new_voice', dateobs=dateobs)
            tasks.twilio.text_everyone.delay(
                render_template('event_new_text.txt', event=event))
            tasks.email.email_everyone.delay(dateobs)
            tasks.slack.slack_everyone.delay(dateobs)


def listen():
    gcn.listen(handler=handle)
