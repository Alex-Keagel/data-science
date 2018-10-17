import json
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark_utils.date_time_utils import timestamp_str_to_date_str, to_timestamp, get_date_n_days_ago
from pyspark_utils.pyspark_general_utils import to_list, to_int, to_float
from flaten_mixpanel_home_events.pyspark_schema_utils import rdd_to_df


# Load sent events
def load_sent_events(events, from_date, to_date, record_example, sqlContext):
    sent_events = events.filter(events.event == "contentitem_sent") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('daysfromeulaapproval', None) != None) \
        .filter(lambda x: x.get('messageid', None) != None) \
        .filter(lambda x: x.get('deviceid', None) != None) \
        .map(lambda x: Row(device_os_version=x.get('deviceosversion', None),
                           device_id=x['deviceid'],
                           message_id=x['messageid'],
                           days_from_eula_approval=x.get('daysfromeulaapproval', None),
                           program_name=x.get('programname', None),
                           carrier_name=x.get('carriername', None),
                           content_item_category=x.get('Category', None),
                           chosen_conversation_rank=x.get('chosenconversationrank', None),
                           device_model=x.get('devicemodel', None),
                           device_os_type=x.get('deviceostype', None),
                           device_vendor=x.get('devicevendor', None),
                           expanded_notification_exists=x.get('expandednotification_exists', None),
                           expanded_notification_is_enabled=x.get('expandednotification_isenabled', False),
                           form_factor=x.get('formfactor', None),
                           home_tier=x.get('hometier', None),
                           installation_source=x.get('installationsource', None),
                           is_pos_customer=x.get('isposcustomer', False),
                           item_subject=to_list(x.get('itemsubject', '')),
                           is_user_in_new_content_exploration_group=x.get('isuserinnewcontentexplorationgroup', False),
                           sent_local_day_of_week=x.get('localdayofweek', None),
                           sent_localtime=to_timestamp(x.get('localtime', None)),
                           message_type=x.get('messagetype', None),
                           min_days_between_notifications=x.get('mindaysbetweennotifications', None),
                           model_fallback_level=x.get('modelfallbacklevel', None),
                           model_id=x.get('modelid', None),
                           model_id_version=x.get('modelidversion', None),
                           model_recommendation_probability=to_float(x.get('modelrecommendationprobability', None)),
                           model_original_model=x.get('modeloriginalmodel', None),
                           notification_copy_original_variant_count=x.get('notificationcopyoriginalvariantcount', None),
                           notification_copy_attributes=x.get('notificationcopyattributes', None),
                           notification_copy_cariant_label=x.get('notificationcopyvariantlabel', None),
                           notification_frequency_model_id=x.get('notificationfrequencymodelid', None),
                           notification_loading_method=x.get('notificationloadingmethod', 'fullappinitialization'),
                           notification_preview_headline=x.get('notificationpreviewheadline', None),
                           notification_preview_message=x.get('notificationpreviewmessage', None),
                           notification_render_type=x.get('notificationrendertype', None),
                           possible_conversations_count=x.get('possibleconversationscount', None),
                           priority_method=x.get('prioritymethod', None),
                           sales_source=x.get('salessource', None),
                           segments_attribute=to_list(x.get('segments_attribute', '')),
                           sent_as_high_priority=x.get('sentashighpriority', False),
                           show_footer=x.get('showfooter', None),
                           source_sender=x.get('source_sender', None),
                           timeline_enabled=x.get('timeline_enabled', False),
                           word_count=x.get('wordcount', None),
                           card_background_attribute=x.get('cardbackground_attribute', None),
                           complexity_attribute=to_int(x.get('complexity_attribute', None)),
                           content_channels_card=x.get('content_channels_card', False),
                           content_channels_notification=x.get('content_channels_notification', False),
                           content_channels_search_results=x.get('content_channels_searchresults', False),
                           contenttype_attribute=to_list(x.get('contenttype_attribute', [])),
                           device_type_mobile=x.get('deviceType_mobile', False),
                           device_type_tablet=x.get('deviceType_Tablet', False),
                           device_features_attribute=to_list(x.get('devicefeatures_attribute', [])),
                           format_attribute=x.get('format_attribute', None),
                           notificationcopy_attribute=to_list(x.get('notificationcopy_attribute', [])),
                           subject_attribute=to_list(x.get('subject_attribute', [])),
                           date=timestamp_str_to_date_str(x.get('timestamp', '')),
                           sent_timestamp=to_timestamp(x.get('timestamp', None)),
                           app_badge_count=x.get('appbadgecount', 0),
                           app_badge_count_method=x.get('appbadgecountmethod', None),
                           app_version=x.get('appversion', None),
                           background_image_attribute=x.get('backgroundimageattribute', None),
                           background_image_original_variant_count=x.get('backgroundimageoriginalvariantcount', None),
                           registration_region=x.get('registrationregion', None),
                           registration_city=x.get("registrationcity", None),
                           registration_country=x.get('registrationcountry', None))).map(lambda x: x.asDict())
    print('sent events loaded')
    return rdd_to_df(sent_events, record_example, sqlContext)


def load_received_events(events, from_date, to_date):
    received_events = events.filter(events.event == "contentitem_received") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('messageid', None) != None) \
        .map(lambda x: Row(received_message_id=x.get('messageid', ''),
                           agent_locale=x.get('agentlocale', ''),
                           device_locale=x.get('devicelocale', ''),
                           received_timestamp=to_timestamp(x['timestamp']))) \
        .toDF(sampleRatio=0.2)
    print('Received events loaded')
    return received_events


# Load received events


def load_dismiss_events(events, from_date, to_date):
    dismiss_events = events.filter(events.event == "contentitem_dismiss") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(dismiss_message_id=x['messageid'],
                           dismiss_timestamp=to_timestamp(x['timestamp']))) \
        .toDF(sampleRatio=0.2)
    print('Dismiss events loaded')
    return dismiss_events


def load_view_events(events, from_date, to_date):
    view_events = events.filter(events.event == "contentitem_view") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('daysfromeulaapproval', None) != None) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(view_message_id=x.get('messageid', None),
                           view_timestamp=to_timestamp(x['timestamp']))) \
        .toDF(sampleRatio=0.2)
    print('View events loaded')
    return view_events


def load_time_on_page_events(events, from_date, to_date):
    time_on_page_events = events.filter(events.event == "contentitem_timeonpage") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(time_on_page_message_id=x['messageid'],
                           time_on_page=round(to_float(x.get('timeonpage', 0.0)), 2))) \
        .toDF(sampleRatio=0.2)
    print('TimeOnPage events loaded')
    return time_on_page_events


def load_page_scroll_events(events, from_date, to_date):
    page_scroll_events = events.filter(events.event == "contentitem_pagescroll") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(page_scroll_message_id=x['messageid'],
                           scroll_depth=to_int(float(x.get('scrolldepth', 0.0))))) \
        .toDF(sampleRatio=0.2)
    grouped_page_scroll_events = page_scroll_events.groupBy(page_scroll_events.page_scroll_message_id) \
        .agg(F.max(page_scroll_events.scroll_depth)) \
        .withColumnRenamed('max(scroll_depth)', 'scroll_depth')
    print('Page scroll events loaded')
    return grouped_page_scroll_events


def load_feedbackmodule_impression_events(events, from_date, to_date):
    feedbackmodule_impression_events = events.filter(events.event == "contentitem_feedbackmodule") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(feedback_module_impression_message_id=x['messageid'],
                           feedback_module_impression_timestamp=to_timestamp(x['timestamp']))) \
        .toDF(sampleRatio=0.2)
    print('FeedbackModule impression events loaded')
    return feedbackmodule_impression_events


def load_feedbackmodule_click_events(events, from_date, to_date):
    feedbackmodule_click_events = events.filter(events.event == "contentitem_click") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('actionid', None) == 'feedbackmodule') \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(feedback_module_click_message_id=x['messageid'],
                           feedback_value=x['feedbackvalue'])) \
        .toDF(sampleRatio=0.2)
    print('FeedbackModule click events loaded')
    return feedbackmodule_click_events


def load_click_events(events, from_date, to_date):
    click_events = events.filter(events.event == "contentitem_click") \
        .filter((events.time >= from_date) & (events.time <= to_date)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .filter(lambda x: x.get('actionid', None) is not None) \
        .map(lambda x: Row(click_message_id=x['messageid'],
                           action=x['actionid'],
                           timestamp=to_timestamp(x['timestamp']))) \
        .toDF(sampleRatio=0.2)
    grouped_click_events_by_messageid = click_events.groupBy('click_message_id') \
        .agg(F.collect_list(F.struct("action", "timestamp"))) \
        .withColumnRenamed('collect_list(named_struct(NamePlaceholder(), action, NamePlaceholder(), timestamp))',
                           'actions').rdd.map(
        lambda x: Row(click_message_id=x['click_message_id'], actions=x['actions'])).toDF(sampleRatio=0.2)
    print('click events loaded')
    return grouped_click_events_by_messageid


def get_topics_possible_click_objects(events, from_date):
    click = events.filter(events.event == 'contentitem_click') \
        .filter(events.time >= from_date) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x.get('istest', False) is False) \
        .filter(lambda x: x.get('messagetype', None) is not None) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(message_type=x['messagetype'].lower(), ActionId=x.get('actionid', ''))) \
        .toDF(sampleRatio=0.2)
    return click.groupBy('message_type').pivot('ActionId').count().fillna(0)


# left join between events1 & events2 by join_id1=join_id2
def binary_join_events(events1, events2, join_id1, join_id2, column_name):
    events1_events2_joined = events1.join(events2, events1[join_id1] == events2[join_id2], 'left')
    events_joined = events1_events2_joined \
        .withColumn(column_name,
                    F.when(events1_events2_joined[join_id2] != "None", 1).otherwise(0)) \
        .withColumn(column_name + '_timestamp',
                    F.when(events1_events2_joined[join_id2] != "None",
                           events1_events2_joined[column_name + '_timestamp']).otherwise(None)) \
    .drop(join_id2)
    print('{} events joined with {} events'.format(join_id1, join_id2))
    return events_joined


def join_sent_received_events(sent, received):
    sent_received_joined = sent.join(received, sent.message_id == received.received_message_id, 'left')
    sent_received = sent_received_joined \
        .withColumn('agent_locale',
                    F.when(sent_received_joined.received_message_id != "None",
                           sent_received_joined.agent_locale).otherwise(None)) \
        .withColumn('device_locale',
                    F.when(sent_received_joined.received_message_id != "None",
                           sent_received_joined.device_locale).otherwise(None)) \
        .withColumn('received_timestamp',
                    F.when(sent_received_joined.received_message_id != "None",
                           sent_received_joined.received_timestamp).otherwise(None)) \
        .withColumn('received',
                    F.when(sent_received_joined.received_message_id != "None",
                           1).otherwise(0)) \
        .drop('received_message_id')
    print('Sent events joined with received events')
    return sent_received


def numeric_join_events(events1, events2, join_id1, join_id2, column_name):
    events1_events2_joined = events1.join(events2, events1[join_id1] == events2[join_id2], 'left')
    events_joined = events1_events2_joined.withColumn(column_name,
                                                      F.when(events1_events2_joined[join_id2] != "None",
                                                             events1_events2_joined[column_name])
                                                      .otherwise(None)) \
        .drop(join_id2)
    print('{} events joined with {} events'.format(join_id1, join_id2))
    return events_joined


def non_numeric_join_events(events1, events2, join_id1, join_id2, column_name):
    events1_events2_joined = events1.join(events2, events1[join_id1] == events2[join_id2], 'left')
    events_joined = events1_events2_joined.withColumn(column_name,
                                                      F.when(events1_events2_joined[join_id2] != "None",
                                                             events1_events2_joined[column_name])
                                                      .otherwise(None)) \
        .drop(join_id2)
    print('{} events joined with {} events'.format(join_id1, join_id2))
    return events_joined


def join_all_events(sent, received, dismiss, view, time_on_page, page_scroll, feedbackmodule_impression,
                    feedbackmodule_click, click):
    sent_received = join_sent_received_events(sent,
                                              received)
    received_dismiss = binary_join_events(sent_received,
                                          dismiss,
                                          'message_id',
                                          'dismiss_message_id',
                                          'dismiss')
    dismiss_view = binary_join_events(received_dismiss,
                                      view,
                                      'message_id',
                                      'view_message_id',
                                      'view')
    view_time_on_page = numeric_join_events(dismiss_view,
                                            time_on_page,
                                            'message_id',
                                            'time_on_page_message_id',
                                            'time_on_page')
    time_on_page_page_scroll = numeric_join_events(view_time_on_page,
                                                   page_scroll,
                                                   'message_id',
                                                   'page_scroll_message_id',
                                                   'scroll_depth')
    page_scroll_feedbackmodule_impression = binary_join_events(time_on_page_page_scroll,
                                                               feedbackmodule_impression,
                                                               'message_id',
                                                               'feedback_module_impression_message_id',
                                                               'feedback_module_impression')
    feedbackmodule_impression_feedbackmodule_click = non_numeric_join_events(page_scroll_feedbackmodule_impression,
                                                                             feedbackmodule_click,
                                                                             'message_id',
                                                                             'feedback_module_click_message_id',
                                                                             'feedback_value')
    feedbackmodule_click_click = non_numeric_join_events(feedbackmodule_impression_feedbackmodule_click,
                                                         click,
                                                         'message_id',
                                                         'click_message_id',
                                                         'actions')
    print('All events were joined by messageId')
    return feedbackmodule_click_click


def get_max_date(events):
    min_date = get_date_n_days_ago(14)
    latest_events = events.filter(events.event == "contentitem_sent").filter(events.time >= min_date)
    latest_events.groupBy('event').agg({'time': "max"}).collect()
