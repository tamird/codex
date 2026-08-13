use super::*;

impl App {
    pub(super) async fn refresh_pending_thread_approvals(&mut self) {
        let side_parent_thread_id = self.active_side_parent_thread_id();
        let channels: Vec<(ThreadId, Arc<Mutex<ThreadEventStore>>)> = self
            .thread_event_channels
            .iter()
            .map(|(thread_id, channel)| (*thread_id, Arc::clone(&channel.store)))
            .collect();

        self.pending_thread_approval_labels.clear();
        for (thread_id, store) in channels {
            if Some(thread_id) == self.active_thread_id || Some(thread_id) == side_parent_thread_id
            {
                continue;
            }

            let has_pending_approvals = store.lock().await.has_pending_thread_approvals();
            if has_pending_approvals {
                let label = self.thread_label(thread_id);
                self.pending_thread_approval_labels.insert(thread_id, label);
            }
        }

        self.publish_pending_thread_approvals();
    }

    fn publish_pending_thread_approvals(&mut self) {
        let mut pending_threads: Vec<_> = self.pending_thread_approval_labels.iter().collect();
        pending_threads.sort_by_key(|(thread_id, _)| thread_id.to_string());
        let threads = pending_threads
            .into_iter()
            .map(|(_, label)| label.clone())
            .collect();
        self.chat_widget.set_pending_thread_approvals(threads);
    }

    pub(super) fn update_pending_thread_approval(
        &mut self,
        thread_id: ThreadId,
        has_pending_approvals: bool,
    ) {
        if !has_pending_approvals
            || Some(thread_id) == self.active_thread_id
            || Some(thread_id) == self.active_side_parent_thread_id()
        {
            if self
                .pending_thread_approval_labels
                .remove(&thread_id)
                .is_some()
            {
                self.publish_pending_thread_approvals();
            }
            return;
        }

        if !self.pending_thread_approval_labels.contains_key(&thread_id) {
            let label = self.thread_label(thread_id);
            self.pending_thread_approval_labels.insert(thread_id, label);
            self.publish_pending_thread_approvals();
        }
    }

    pub(super) fn update_pending_thread_approval_label(&mut self, thread_id: ThreadId) {
        if !self.pending_thread_approval_labels.contains_key(&thread_id) {
            return;
        }

        let label = self.thread_label(thread_id);
        if self.pending_thread_approval_labels.get(&thread_id) == Some(&label) {
            return;
        }

        self.pending_thread_approval_labels.insert(thread_id, label);
        self.publish_pending_thread_approvals();
    }
}
