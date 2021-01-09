#! /bin/env python3
import csv
import json
import logging
import humanize
import time
import sys
import os
from tqdm import tqdm


if __name__ == '__main__':
    start = time.perf_counter()
    # csv.field_size_limit(sys.maxsize)
    csv.field_size_limit(2147483647)
    from collections import Counter, dict_items

    # cached from greps or whatever
    # file_to_parse = 'ol_dump_authors_2020-11-30.txt'
    author_keys = dict_items(
        [('name', 8218941), ('personal_name', 4912172), ('last_modified', 8219164), ('key', 8219164), ('type', 8219164), ('revision', 8219164),
         ('created', 3602619), ('latest_revision', 2455696), ('alternate_names', 45730), ('title', 120079), ('bio', 21361), ('death_date', 383430),
         ('photos', 39661), ('birth_date', 1411748), ('remote_ids', 164162), ('date', 17780), ('links', 9777), ('website', 2972), ('location', 2250),
         ('wikipedia', 1814), ('fuller_name', 1022), ('entity_type', 971), ('create', 118), ('numeration', 167), ('photograph', 780), ('comment', 18),
         ('source_records', 38), ('ocaid', 7), ('works', 12), ('role', 104), ('covers', 6), ('number_of_pages', 14), ('series', 3),
         ('lc_classifications', 19), ('edition_name', 4), ('languages', 16), ('subjects', 20), ('publish_country', 16), ('oclc_numbers', 17),
         ('publishers', 17), ('authors', 20), ('publish_places', 16), ('pagination', 16), ('dewey_decimal_class', 8), ('lccn', 19),
         ('publish_date', 17), ('subject_place', 7), ('id_librarything', 1), ('id_wikidata', 1), ('id_viaf', 1), ('tags', 3), ('by_statement', 9),
         ('other_titles', 1), ('notes', 6), ('subtitle', 5), ('title_prefix', 5), ('marc', 1), ('subject_time', 1), ('genres', 2),
         ('contributions', 1), ('     _date', 1), ('body', 1), ('website_name', 1)])

    # file_to_parse = 'ol_dump_works_2020-11-30.txt'
    works_keys = dict_items(
        [('title', 21531195), ('created', 21531431), ('last_modified', 21531431), ('latest_revision', 21531431), ('key', 21531431),
         ('authors', 20427328), ('type', 21531431), ('revision', 21531431), ('covers', 6173139), ('subjects', 12712888), ('description', 818003),
         ('subtitle', 573185), ('subject_places', 5600638), ('subject_people', 1787775), ('subject_times', 1389254), ('cover_edition', 1998),
         ('links', 21410), ('lc_classifications', 2378305), ('first_publish_date', 4459379), ('dewey_number', 1158612), ('first_sentence', 72714),
         ('excerpts', 48267), ('location', 31), ('remote_ids', 6), ('ospid', 1), ('notes', 2), ('other_titles', 1), ('genres', 4),
         ('original_languages', 5), ('number_of_editions', 2), ('translated_titles', 4), ('notifications', 1), ('ocaid', 1), ('series', 1),
         ('permission', 1), ('table_of_contents', 1)])

    all_data_keys = dict_items(
        [('body', 614), ('title', 51196993), ('last_modified', 61320756), ('key', 61320756), ('type', 61320756), ('revision', 61320756),
         ('/type/page', 254), ('name', 8311648), ('personal_name', 4912387), ('/type/author', 8219164), ('created', 52487265),
         ('latest_revision', 53963407), ('alternate_names', 45741), ('bio', 21373), ('death_date', 383463), ('photos', 39733),
         ('birth_date', 1411834), ('remote_ids', 164350), ('date', 17788), ('links', 298001), ('location', 1338667), ('/type/redirect', 699331),
         ('website', 3201), ('wikipedia', 1818), ('/type/delete', 1234450), ('limit', 1626), ('fuller_name', 1035), ('entity_type', 977),
         ('publishers', 28234307), ('physical_format', 7931635), ('subtitle', 12427332), ('identifiers', 8612612), ('isbn_13', 11094969),
         ('languages', 25547057), ('number_of_pages', 22323664), ('isbn_10', 14084098), ('publish_date', 28570180), ('authors', 44619055),
         ('works', 27115825), ('subjects', 31883127), ('/type/edition', 29542254), ('edition_name', 4796458), ('oclc_numbers', 7779513),
         ('series', 6738586), ('covers', 15020162), ('lc_classifications', 16759633), ('publish_country', 19031292), ('by_statement', 14042327),
         ('publish_places', 19350087), ('pagination', 18188106), ('dewey_decimal_class', 5919797), ('notes', 13450807), ('lccn', 10854288),
         ('source_records', 16447457), ('subject_place', 3046099), ('url', 529658), ('contributions', 8095366), ('uri_descriptions', 209021),
         ('uris', 211198), ('ocaid', 3717251), ('other_titles', 2615901), ('subject_time', 689884), ('genres', 1329888), ('description', 1861745),
         ('ia_loaded_id', 283118), ('ia_box_id', 468647), ('weight', 2840321), ('physical_dimensions', 2852361), ('local_id', 1112095),
         ('title_prefix', 1429481), ('classifications', 684833), ('isbn_invalid', 59561), ('work_title', 261510), ('first_sentence', 582583),
         ('table_of_contents', 1043462), ('contributors', 51737), ('copyright_date', 103993), ('work_titles', 614961), ('coverimage', 3323),
         ('translated_from', 11911), ('translation_of', 12032), ('scan_records', 7626), ('scan_on_demand', 44603), ('oclc_number', 606034),
         ('full_title', 2853918), ('language', 6514), ('download_url', 196), ('create', 875), ('original_isbn', 3969), ('subject_people', 2099892),
         ('subject_places', 6607270), ('subject_times', 1672112), ('openlibrary', 35944), ('isbn_odd_length', 1622), ('/type/rawtext', 47),
         ('string_fulltext_items_only', 36), ('lang', 956), ('string_narrow_criteria', 36), ('string_publisher', 36), ('string_stats', 33),
         ('string_go', 66), ('string_description', 199), ('string_search_fulltext', 36), ('string_title', 169), ('string_date_range', 35),
         ('string_search_catalog', 36), ('string_advanced_search', 73), ('string_isbn', 36), ('string_author', 37), ('ns', 956),
         ('string_subject', 37), ('string_scannable', 52), ('/type/i18n', 966), ('string_move_up', 33), ('string_pretitle', 58),
         ('string_edit_summary', 32), ('string_save', 72), ('string_modify_view', 29), ('string_type_search', 31), ('string_edit_pretitle', 29),
         ('string_delete', 35), ('string_move_down', 33), ('string_modify_edit', 29), ('string_change', 33), ('string_edit_title', 32),
         ('string_view', 127), ('string_editing', 33), ('string_add_property', 32), ('string_modify_template', 29), ('string_change_type', 33),
         ('string_history', 188), ('string_add_image', 33), ('string_preview', 34), ('string_page_type', 31), ('string_clone_book', 31),
         ('string_who', 64), ('string_what', 64), ('string_when', 64), ('string_viewing_history', 33), ('string_edit', 99), ('string_number', 34),
         ('string_compare', 34), ('string_develop_site', 36), ('string_find_books', 36), ('string_example_search', 37), ('string_code_repo', 35),
         ('string_tagline', 35), ('string_recent_changes', 66), ('string_build_library', 35), ('string_open_library', 90), ('string_about_lib', 34),
         ('string_about_tech', 37), ('string_documentation', 37), ('string_add_a_book', 72), ('string_about_project', 37), ('string_guided_tour', 82),
         ('string_arrow', 27), ('string_bug_tracking', 36), ('string_body', 62), ('comment', 452), ('code', 468), ('/type/language', 466),
         ('status', 324), ('addresses', 249), ('country', 323), ('telephone', 267), ('ip_ranges', 186), ('contact_email', 299),
         ('registered_on', 260), ('contact_person', 297), ('contact_title', 279), ('/type/library', 325), ('/type/subject', 91400),
         ('/type/work', 21531431), ('cover_edition', 1999), ('first_publish_date', 4459380), ('dewey_number', 1158612), ('excerpts', 48267),
         ('collections', 225), ('string_remember_me', 40), ('string_login', 93), ('string_forgot_password', 76), ('string_create_new_account', 77),
         ('string_password', 78), ('string_username', 77), ('string_try_language', 28), ('string_change_language', 119), ('string_click_here', 28),
         ('string_change_language_statement4', 24), ('string_change_language_statement2', 21), ('string_change_language_statement3', 21),
         ('string_begin_editing', 28), ('string_change_language_statement', 21), ('string_wiki_doc', 26), ('string_learn_more', 87),
         ('string_unmodified', 30), ('string_revision', 30), ('string_diff', 58), ('string_modified', 30), ('string_added', 30),
         ('string_diff_title', 30), ('string_differences', 31), ('string_legend', 28), ('string_removed', 29), ('string_name', 122),
         ('string_type', 31), ('string_is_unique', 30), ('macro', 126), ('/type/macro', 126), ('/type/template', 300), ('plugin', 164),
         ('numeration', 172), ('photograph', 797), ('isbn', 291), ('string_login_stmt', 36), ('string_ol_id', 34), ('string_refine_results', 34),
         ('string_cant_find', 34), ('string_scanned_books_only', 34), ('string_contain_words', 33), ('string_unknown', 35),
         ('string_has_fulltext', 84), ('string_pre1920', 35), ('string_selected_facets', 27), ('string_by', 32), ('string_search', 37),
         ('string_not_available', 64), ('string_more_results', 36), ('string_facet_year', 78), ('string_fulltext', 34), ('string_search_results', 35),
         ('string_available', 34), ('string_after2000', 36), ('string_property_name', 27), ('string_expected_type', 26), ('string_is_primitive', 27),
         ('string_backreferences', 23), ('string_properties', 29), ('/type/usergroup', 13), ('purchase_url', 243), ('has_fulltext', 32), ('m', 364),
         ('volumes', 27), ('string_website', 68), ('string_death_date', 34), ('string_entity_type', 24), ('string_birth_date', 34),
         ('string_fuller_name', 30), ('string_personal_name', 31), ('string_numeration', 28), ('string_location', 33), ('string_books', 35),
         ('string_by_author', 33), ('string_bio', 33), ('string_alternate_names', 33), ('language_code', 22), ('string_isbn_10', 32),
         ('string_other_titles', 25), ('string_ocaid', 27), ('string_isbn_13', 32), ('string_number_of_pages', 33),
         ('string_physical_dimensions', 32), ('string_pagination', 30), ('string_dewry_decimal_class', 25), ('string_author_create_new', 31),
         ('string_no_toc', 28), ('string_source_records', 10), ('string_gbs', 25), ('string_oclc_numbers', 30), ('string_author_dropdown', 28),
         ('string_physical_format', 33), ('string_first_sentence', 34), ('string_copyright_date', 25), ('string_publish_date', 33),
         ('string_contributors', 28), ('string_genres', 31), ('string_authors', 35), ('string_author_create', 33), ('string_publishers', 35),
         ('string_lc_classifications', 30), ('string_contributions', 28), ('string_weight', 34), ('string_subjects', 32), ('string_purchase_url', 21),
         ('string_by_statement', 21), ('string_notes', 33), ('string_size', 35), ('string_title_prefix', 31), ('string_subtitle', 31),
         ('string_add_it', 34), ('string_publish_places', 33), ('string_lccn', 27), ('string_download_url', 22), ('string_series', 32),
         ('string_languages', 36), ('string_edition_name', 30), ('string_work_titles', 26), ('string_dewey_decimal_class', 26),
         ('string_table_of_contents', 34), ('string_i18n', 34), ('string_add_new_key', 31), ('string_select_translation', 31), ('string_add', 31),
         ('string_available_translations', 31), ('string_add_new_namespace', 29), ('string_add_new_language', 31), ('string_i18n_strings', 28),
         ('string_email', 111), ('string_users_activity', 36), ('string_displayname', 36), ('lending_region', 53), ('kind', 46), ('/type/type', 47),
         ('v', 4), ('edition', 108), ('ia_id', 108), ('volume_number', 108), ('string_published_in', 83), ('string_updated_by', 36),
         ('string_whats_new', 36), ('string_full_text', 35), ('string_october', 49), ('string_last_modified', 38), ('expected_type', 7),
         ('property_name', 7), ('/type/backreference', 7), ('string_submit', 87), ('string_email_not_registered', 37), ('string_password_sent', 37),
         ('string_forgot_password_statement', 31), ('string_reset_password', 35), ('string_password_sent_statement2', 29),
         ('string_password_sent_statement', 22), ('string_reset_password_statement', 30), ('content_type', 34), ('rawtext', 4),
         ('string_current_password', 37), ('string_confirm_password', 74), ('string_add_to_site', 29), ('string_trranslate_ol', 29),
         ('string_preferred_language', 38), ('string_passwords_did_not_match', 74), ('string_change_password', 37), ('string_incorrect_password', 37),
         ('string_login_preferences', 33), ('string_template_preferences', 32), ('string_users_preferences', 36), ('string_template_root', 30),
         ('string_translate', 33), ('string_preferences', 90), ('string_new_password', 38), ('string_account_options', 37),
         ('string_register_help', 36), ('string_register', 37), ('string_username_already_exists', 37), ('string_display_name', 37),
         ('string_register_help_title', 36), ('stats', 10), ('/type/home', 11), ('string_download_plain_text', 30), ('string_local_library', 30),
         ('string_download_pdf', 30), ('string_see_all_files', 30), ('string_borrow', 30), ('string_view_book', 30), ('string_buy', 30),
         ('string_download_djvu', 30), ('string_browse', 29), ('string_second', 33), ('string_minutes', 33), ('string_hours', 33),
         ('string_april', 19), ('string_hour', 33), ('string_minute', 33), ('string_8', 16), ('string_9', 16), ('string_may', 19),
         ('string_days', 33), ('string_3', 16), ('string_0', 16), ('string_7', 16), ('string_4', 16), ('string_microsecond', 10),
         ('string_january', 18), ('string_ago', 31), ('string_2', 16), ('string_july', 19), ('string_hence', 28), ('string_millisecond', 10),
         ('string_august', 18), ('string_february', 18), ('string_microseconds', 10), ('string_november', 18), ('string_march', 18),
         ('string_from_now', 8), ('string_6', 16), ('string_september', 13), ('string_seconds', 33), ('string_day', 11), ('string_june', 19),
         ('string_milliseconds', 10), ('string_septemeber', 8), ('string_1', 16), ('string_5', 16), ('string_december', 18), ('role', 104),
         ('dimensions', 4), ('coverid', 1), ('bookweight', 10), ('string_basics', 26), ('permission', 35), ('child_permission', 5),
         ('/type/i18n_page', 15), ('library_of_congress_name', 10), ('urn_prefix', 7), ('id_location', 7), ('source_ocaid', 7), ('/type/local_id', 7),
         ('/type/doc', 9), ('properties', 36), ('readers', 6), ('writers', 7), ('admins', 5), ('/type/permission', 9), ('/type/volume', 107),
         ('price', 3), ('by_statements', 1), ('backreferences', 8), ('string_request_scan', 1), ('string_older', 29), ('string_actions', 29),
         ('string_path', 29), ('string_newer', 29), ('mailing', 1), ('title2', 2), ('intro', 2), ('/type/about', 1), ('string_macro', 32),
         ('string_upload', 51), ('string_bpl', 1), ('string_prev', 52), ('string_deleted', 53), ('string_internet_archive', 50),
         ('string_page_does_not_exist', 52), ('string_next', 53), ('string_create_it', 53), ('string_about_us', 56), ('string_welcome_user', 54),
         ('string_site_title', 49), ('string_logout', 54), ('string_contact_us', 55), ('string_powered_by_infogami', 47), ('string_back_to_ol', 45),
         ('string_not_found', 52), ('string_hello', 53), ('string_create', 54), ('author_names', 7), ('string_subtitles', 16),
         ('string_by_statements', 11), ('barcode', 3), ('id_librarything', 1), ('id_wikidata', 1), ('id_viaf', 1), ('string_book', 1),
         ('/type/series', 3), ('/type/content', 1), ('ospid', 1), ('news', 9), ('tags', 3), ('members', 11), ('/type/scan_record', 2), ('loation', 2),
         ('roles', 1), ('/type/object', 2), ('publisher', 2), ('marc', 1), ('string_get', 1), ('lc_classification', 6), ('original_languages', 5),
         ('editions', 1), ('/type/collection', 3), ('number_of_editions', 2), ('translated_titles', 4), ('scan_status', 1), ('displayname', 1),
         ('/type/user', 2), ('notifications', 1), ('string_briefly_describe_the_changes_you_have_made', 1), ('/type/scan_location', 1),
         ('string_', 1), ('/type/uri', 1), ('string_11', 1), ('string_121212', 1), ('string_index', 2), ('string_IA', 1),
         ('string_ Advanced Search', 1), ('string_Change Password', 1), ('string_Internet_Archive', 1), ('type_key', 1), ('demonym', 1),
         ('longitude', 1), ('marc_names', 1), ('latitude', 1), ('/type/place', 1), ('string_26061980', 1), ('string_thank_you', 1),
         ('string_dioseol2012', 1), ('column_2', 1), ('column_1', 1), ('statement', 1), ('column_3', 1), ('oclc', 1), ('test', 1),
         ('string_Fise elevi', 1), ('unique', 1), ('     _date', 1), ('subject_facet', 1), ('numer_of_pages', 1), ('string_ACCOUNTING', 1),
         ('string_site_subtitle', 1), ('cmd', 1), ('cmd2', 1), ('encrypted2', 1), ('hosted_button_id', 1), ('website_name', 1), ('property', 1),
         ('string_foo', 1)])

    file_to_parse = os.path.join('g://', 'openlibrary', 'ol_dump_2020-11-30.txt')
    pbar = tqdm(total=61320756)
    logging.info(f"Starting processing: {file_to_parse}")
    all_data = {}
    limit = 100
    i = 0
    keys = set()
    c = Counter()
    with open(file_to_parse, 'r', encoding='utf-8') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        for row in reader:
            data = json.loads(row[4])
            c.update(data.keys())
            c.update([row[0]])
            pbar.update(1)
    pbar.close()

    end = time.perf_counter()
    logging.info(f"Total time: {end - start:0.4f} seconds")
    logging.info(f"Total time: {humanize.naturaltime(end - start)}")
    print(c.items())
