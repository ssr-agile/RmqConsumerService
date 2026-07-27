-- Generated from {schema} golden dump.
-- Runtime placeholder: {schema}
SET LOCAL search_path = {schema}, pg_catalog;
SET LOCAL check_function_bodies = false;

ALTER TABLE ONLY {schema}.ax_homebuild_responsibility
    ADD CONSTRAINT ax_homebuild_responsibility_homebuild_id_fkey FOREIGN KEY (homebuild_id) REFERENCES {schema}.ax_homebuild_master(homebuild_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_homebuild_sd_responsibility
    ADD CONSTRAINT ax_homebuild_sd_responsibility_homebuild_id_fkey FOREIGN KEY (homebuild_id) REFERENCES {schema}.ax_homebuild_saved(homebuild_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_hp_user_level_widget
    ADD CONSTRAINT ax_hp_user_level_widget_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_pages(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_page_responsibility
    ADD CONSTRAINT ax_page_responsibility_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_pages(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_page_sd_responsibility
    ADD CONSTRAINT ax_page_sd_responsibility_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_page_saved(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_published
    ADD CONSTRAINT ax_widget_published_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_pages(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_published
    ADD CONSTRAINT ax_widget_published_parent_widget_id_fkey FOREIGN KEY (parent_widget_id) REFERENCES {schema}.ax_widget_saved(widget_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_responsibility
    ADD CONSTRAINT ax_widget_responsibility_widget_id_fkey FOREIGN KEY (widget_id) REFERENCES {schema}.ax_widget(widget_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_saved
    ADD CONSTRAINT ax_widget_saved_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_page_saved(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.axresponse
    ADD CONSTRAINT axresponse_requestid_fkey FOREIGN KEY (requestid) REFERENCES {schema}.axrequest(requestid);
