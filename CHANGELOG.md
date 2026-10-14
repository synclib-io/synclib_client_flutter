## 0.0.2

* Merkle: drop local copies of rows the server deleted (`deleted_row_ids`) before the scoped recheck; apply LWW tombstones as deletes; send `base_row_hash` so a stale copy (e.g. a fast device clock) can't win LWW; fail the run (`incomplete`) when a table can't be hashed.
* Unscoped channels keep a stored pull cursor per table (`_synclib_cursors`), committed from the server's `sync_complete`, instead of the local `MAX(seqnum)`, which acked writes could push past unpulled rows. Devices re-pull once from 0.
* A second write to a row whose push is in flight is no longer lost when the first push is acked: a changed queue entry is re-queued under a fresh seqnum.
* A sync requested while one runs now runs once more afterwards instead of being dropped; handle `sync_error`.

## 0.0.1

* TODO: Describe initial release.
