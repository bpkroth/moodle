<?php
// vim: set et sw=4 ts=4:
// This file is part of Moodle - http://moodle.org/
//
// Moodle is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Moodle is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Moodle.  If not, see <http://www.gnu.org/licenses/>.

/**
 * MongoDB based session handler.
 *
 * Similar to the sessions_db handler's advantages, MongoDB allows for more 
 * granular per-transaction durability relaxations in order to help speed up 
 * write activities for data that perhaps doesn't require strict persistence.
 *
 * Additionally, unlike Memcached, MongoDB provides access control which is 
 * useful in a shared environment.
 *
 * @package    core
 * @copyright  2014 Brian Kroth {@link http://cae.wisc.edu/~bpkroth}
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

namespace core\session;

defined('MOODLE_INTERNAL') || die();

/**
 * MongoDB based session handler.
 *
 * See above for NOTEs.
 *
 * @package    core
 * @copyright  2014 Brian Kroth {@link http://cae.wisc.edu/~bpkroth}
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */
class mongodb extends handler {
    /** @var $sessdata_id sessdata record id */
    protected $sessdata_id = null;

    /** @var $timemodified session timemodified */
    protected $timemodified = null;

    /** @var string $lasthash hash of the session data content */
    protected $lasthash = null;

    /** @var $sesslock_id sesslock record id */
    protected $sesslock_id = null;

    /**
     * The Connection object
     * @var MongoClient
     */
    protected $connection = false;

    /**
     * The Database Object
     * @var MongoDB
     */
    protected $database;

    /**
     * The sessdata Collection object
     * @var MongoCollection
     */
    protected $sessdata_collection;

    /**
     * The sesslock Collection object
     * @var MongoCollection
     */
    protected $sesslock_collection;

    /**
     * Determines if and what safe setting is to be used.
     * @var bool|int
     */
    protected $usesafe = false;

    /** @var bool $failed session read/init failed, do not write back to DB */
    protected $failed = false;

    /** @var int $acquiretimeout how long to wait for session lock */
    protected $acquiretimeout = 120;

    /** @var int $lock_expire how long to leave stale locks before automatically removing them */
    protected $lock_expire = 7200;

    /** @var int $sessdata_expire how long to leave stale sessdata before automatically removing them */
    protected $sessdata_expire = 28800;

    /** @var string session_serializer that was requested */
    protected $session_serializer = 'php';

    /**
     * Create new instance of handler.
     */
    public function __construct() {
        global $CFG;

        if (!empty($CFG->session_mongodb_acquire_lock_timeout)) {
            $this->acquiretimeout = (int)$CFG->session_mongodb_acquire_lock_timeout;
        }
        if (!empty($CFG->session_mongodb_lock_expire)) {
            $this->lock_expire = (int)$CFG->session_mongodb_lock_expire;
        }
        if (!empty($CFG->session_mongodb_sessdata_expire)) {
            $this->sessdata_expire = (int)$CFG->session_mongodb_sessdata_expire;
        }

        if (isset($CFG->session_serializer)) {
            $this->session_serializer = $CFG->session_serializer;
        }

        // Adapted from cache/stores/mongodb/lib.php
        $server = 'mongodb://127.0.0.1:27017';
        if (isset($CFG->session_mongodb_server)) {
            $server = $CFG->session_mongodb_server;
        }
        $databasename = 'msessdata';
        if (isset($CFG->session_mongodb_database)) {
            $databasename = $CFG->session_mongodb_database;
        }
        $options = array();
        if (!empty($CFG->session_mongodb_username)) {
            $options['username'] = $CFG->session_mongodb_username;
        }
        if (!empty($CFG->session_mongodb_password)) {
            $options['password'] = $CFG->session_mongodb_password;
        }
        if (!empty($CFG->session_mongodb_replicaset)) {
            $options['replicaSet'] = $CFG->session_mongodb_replicaset;
        }
        if (!empty($CFG->usesafe)) {
            $this->usesafe = $CFG->session_mongodb_usesafe;
        }

        try {
            // NOTE: The Mongo class is deprecated.  The mongodb cachestore should probably also be fixed up.
            $this->connection = new \MongoClient($server, $options);
            $this->database = $this->connection->selectDB($databasename);
            $this->sessdata_collection = $this->database->selectCollection('sessdata');
            $this->sesslock_collection = $this->database->selectCollection('sesslock');
        } catch (\MongoConnectionException $e) {
            // We only want to catch MongoConnectionExceptions here.
        }
    }

    /**
     * Simple wrapper function to check MongoDB write request response.
     *
     * See Also: cache/stores/mongodb/lib.php for similar mantras.
     *
     * @var $result
     * @returns bool
     */
    public static function check_mongodb_response($result) {
        if ($result === true) {
            // Safe mode.
            return true;
        } elseif (is_array($result)) {
            if (empty($result['ok']) || isset($result['err'])) {
                return false;
            }
            return true;
        }
        // Who knows?
        return false;
    }

    /**
     * Make sure that the collections have the necessary indexes.
     * @param $force_reindex Whether or not to force a reindex.
     * @returns bool
     */
    public function setup_collections_indexes($force_reindex = false) {
        global $CFG;

        if (empty($this->database)) {
            return false;
        }

        # Check to see if we need to reindex.
 
        $reindex_timestamp = 0; # by default don't reindex
        if (!empty($CFG->session_mongodb_reindex_timestamp)) {
            $reindex_timestamp = (int)$CFG->session_mongodb_reindex_timestamp;
        }

        $last_reindex_timestamp = null; # but assume we need to if we can't tell when it was last done
        $config_collection = $this->database->selectCollection('config');
        /* TODO: Trying without this for now.
        $result = $config_collection->ensureIndex(array('key' => 1), array(
            'safe' => $this->usesafe,
            'w' => ($this->usesafe) ? 1 : 0,
            'unique' => true,
            'name' => 'idx_key_unique'
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-ensureIndex-problem', 'error');
        }
         */

        $config_doc = $config_collection->findOne(array('key' => 'last_reindex_timestamp'), array('value'));
        if (!empty($config_doc) && !empty($config_doc['value'])) {
            $last_reindex_timestamp = $config_doc['value'];
        }

        if (!$force_reindex && !empty($last_reindex_timestamp) && $last_reindex_timestamp >= $reindex_timestamp) {
            return;
        }

        # NOTE: Both expireAfterSeconds indexes had this comment originally:
            # Set an upperbound on how long session data should remain in the 
            # db before being automatically purged.
            # NOTE: other mechanisms should also be handling this cleanup 
            # before that and can set a more dynamic time period (this one is 
            # fixed once the index is created) if they choose.  This is simply 
            # a last result basic sanity check and cleanup effort.
            # NOTE: The current max idle session time moodle exposes is 4h.
        # Instead I decided to just force the admin to call this function when 
        # they change those values if they want to have different expiration 
        # periods.
        # Alternatively, we could skip expiration in the MongoDB server 
        # entirely and do it all in the client, but I think that would be more 
        # prone to contention and extra client side operations.

        # Additionally, if we're only going to be doing this section every once 
        # in a while, we should probably be willing to take the hit to double 
        # check the responses for errors, so we skip the usesafe check here.

        #
        # Setup the sessdata collection indices.
        #
        $result = $this->sessdata_collection->deleteIndexes();
        # Need to ignore this result in case the collection didn't exist yet.
        #if (!self::check_mongodb_response($result)) {
        #    error_log(print_r($result, true));
        #    throw new exception('mongodb-deleteIndexes-problem', 'error');
        #}
        # Here's an index to enforce uniqueness constraints on the sid field.
        $result = $this->sessdata_collection->ensureIndex(array('sid' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'unique' => true,
            'dropDups' => true,
            'name' => 'idx_sid_unique'
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-ensureIndex-problem', 'error');
        }
        /* Overkill and too much maintenance?
        # We should only ever need to perform point lookups of session ids, so 
        # hash indexes should be most efficient, so add one of those too.
        $result = $this->sessdata_collection->ensureIndex(array('sid' => 'hashed'), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            #'unique' => true,  # hash indexes currently don't support unique constraints
            'name' => 'idx_sid_hashed'
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-ensureIndex-problem', 'error');
        }
         */
        $result = $this->sessdata_collection->ensureIndex(array('timemodified' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'expireAfterSeconds' => $this->sessdata_expire,
            'name' => 'idx_timemodified'
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-ensureIndex-problem', 'error');
        }

        #
        # Setup the sesslock collection.
        #
        $result = $this->sesslock_collection->deleteIndexes();
        # Need to ignore this result in case the collection didn't exist yet.
        #if (!self::check_mongodb_response($result)) {
        #    error_log(print_r($result, true));
        #    throw new exception('mongodb-deleteIndexes-problem', 'error');
        #}
        # Here's an index to enforce uniqueness constraints on the sid field.
        $result = $this->sesslock_collection->ensureIndex(array('sid' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'unique' => true,
            'dropDups' => true,
            'name' => 'idx_sid_unique'
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-ensureIndex-problem', 'error');
        }
        /* Overkill and too much maintenance?
        # We should only ever need to perform point lookups of session ids, so 
        # hash indexes should be most efficient, so add one of those too.
        $result = $this->sesslock_collection->ensureIndex(array('sid' => 'hashed'), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            #'unique' => true,  # hash indexes currently don't support unique constraints
            'name' => 'idx_sid_hashed'
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-ensureIndex-problem', 'error');
        }
         */
        $result = $this->sesslock_collection->ensureIndex(array('timemodified' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'expireAfterSeconds' => $this->lock_expire,
            'name' => 'idx_timemodified'
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-ensureIndex-problem', 'error');
        }

        # All done, make a record of that, so we don't have to do this again later.
        $result = $config_collection->update(
            array('key' => 'last_reindex_timestamp'),
            array('key' => 'last_reindex_timestamp', 'value' => time()),
            array(
                #'w' => ($this->usesafe) ? 1 : 0,
                #'safe' => $this->usesafe,
                'upsert' => true    # NOTE: depends on the idx_key_unique index defined above.
            )
        );
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-update-problem', 'error');
        }
        if (is_array($result) && $result['n'] != 1) {
            error_log(print_r($result, true));
            throw new exception('mongodb-update-problem', 'error');
        }
        return true;
    }

    /**
     * Init session handler.
     */
    public function init() {
        # DONE: We may want to move all of these ensureIndex() collection setup 
        # calls to something that's a bit more periodic, or perhaps done by the 
        # administrator prior to using this session store.
        $this->setup_collections_indexes();

        $result = session_set_save_handler(array($this, 'handler_open'),
            array($this, 'handler_close'),
            array($this, 'handler_read'),
            array($this, 'handler_write'),
            array($this, 'handler_destroy'),
            array($this, 'handler_gc'));
        if (!$result) {
            throw new exception('mongodbsessionhandlerproblem', 'error');
        }
    }

    /**
     * Check for existing session with id $sid.
     *
     * Note: this verifies the storage backend only, not the actual session records.
     *
     * @param string $sid
     * @return bool true if session found.
     */
    public function session_exists($sid) {
        return ($this->sessdata_collection->count(array('sid'=>$sid)));
    }

    /**
     * Kill all active sessions, the core sessions table is
     * purged afterwards.
     */
    public function kill_all_sessions() {
        try {
            $result = $this->sessdata_collection->remove();
            if (!self::check_mongodb_response($result)) {
                error_log(print_r($result, true));
                throw new exception('mongodb-remove-problem', 'error');
            }

            $result = $this->sesslock_collection->remove();
            if (!self::check_mongodb_response($result)) {
                error_log(print_r($result, true));
                throw new exception('mongodb-remove-problem', 'error');
            }
        } catch (\MongoException $ignored) {
            // Do not show any warnings - might be during upgrade/installation.
        }
        return;
    }

    /**
     * Kill one session, the session record is removed afterwards.
     * @param string $sid
     */
    public function kill_session($sid) {
        $result = $this->sessdata_collection->remove(array('sid'=>$sid), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-remove-problem', 'error');
        }

        $result = $this->sesslock_collection->remove(array('sid'=>$sid), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-remove-problem', 'error');
        }

        return;
    }

    /**
     * A function to acquire a lock document.
     */
    private function get_session_lock($sid) {
        # TODO: Hmm, should we be throwing an error in the case that it appears 
        # that we're already locked, or should we attempt to reacquire a lock?  
        # That might lead to deadlock situations.
        if (!empty($this->sesslock_id)) {
            throw new coding_exception('sesslock_id is already set.');
        }

        $lockdoc = array(
            'sid' => $sid   # NOTE: This should be a unique indexed field, per above.
        );
        $start_time = time();
        do {
            # Use the most recent time in our document.
            $lockdoc['timemodified'] = time();
            # And make sure that we don't have an _id lingering that would 
            # prevent new insert attempts.
            unset($lockdoc['_id']);

            # try to insert the sesslock document
            $result = $this->sesslock_collection->insert($lockdoc, array(
                # NOTE: We can't usesafe=false here, since we need to be able 
                # to check the response to see if someone else beat us to the lock 
                # acquisition.
                #'safe' => $this->usesafe,
                #'w' => ($this->usesafe) ? 1 : 0,
            ));
            if (!self::check_mongodb_response($result)) {
                # If it fails, check to see if we've timedout while waiting for the lock.
                if (time() - $start_time >= $this->acquiretimeout) {
                    break;
                }
                # Else, wait a moment and try again.
                # FIXME? Why 1ms ... no real reason.  I tried to reason out some 
                # reasonable backoff parameters and acceptable wait times 
                # without giving up too much responsiveness or overwhelming the 
                # system, but this ultimately seemed to be good enough.  
                # Usually our target page load times seem to be around 100ms, 
                # so we could guess that we're coming in at half of that (50ms) 
                # and that we have half of that (25ms) left to wait, but that 
                # seemed to long to wait in the optimal case that we came in 
                # just before the page finished.  Also note that there is no 
                # fairness in this scheme since whoever gets there first gets 
                # the lock, so it could lead to starvation.
                usleep(1000);  # 1 ms
            }
            else {
                // lock acquired - save the id, which will cause us to exit this loop
                $this->sesslock_id = (string)$lockdoc['_id'];
            }
        } while (empty($this->sesslock_id));

        if (empty($this->sesslock_id)) {
            throw new \dml_sessionwait_exception();
        }

        return $this->sesslock_id;
    }

    /**
     * A function to release a lock document.
     *
     * NOTE: Currently releases the lock document by and _id that was saved in
     * the get_session_lock() function, though it could possibly release by
     * sid, but that would probably interact poorly in the case that something 
     * removed this session's lock, acquired one of its own under the same sid, 
     * and then was trying to do work.  Then this one would make matters worse 
     * by later on removing the other handler's lock allowing a third party to 
     * starting working as well.
     */
    private function release_session_lock() {
        if (empty($this->sesslock_id)) {
            throw new coding_exception('sesslock_id is not set.');
        }

        $lockdoc = array(
            '_id' => new \MongoId($this->sesslock_id)
        );
        $result = $this->sesslock_collection->remove($lockdoc, array(
            # These locks aren't removed on disconnect like they are for MySQL, 
            # so we should probably try a little harder to make sure they get 
            # released.
            # Then again, all callers just ignore the problem anyways, so I'm 
            # not sure what good it is to wait on this.
            # DONE: Perhaps we should error_log() the response instead, or just 
            # skip it entirely and hope that it works itself out.
            # Besides this generally (close(), but not on destroy()) happens 
            # after the output stream has already been closed, so we usually 
            # can't inform the user anyways.
            'safe' => $this->usesafe,
            'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            error_log('Failed to remove mongodb sesslock '.$this->sesslock_id);
            #throw new exception('mongodb-remove-problem', 'error');
        }
        #if ($result['n'] == 0) {
        #    error_log(print_r($result, true));
        #    error_log('Failed to remove mongodb sesslock '.$this->sesslock_id);
        #    #throw new exception('mongodb-remove-problem', 'error');    # perhaps something else released the lock
        #}
        
        $this->sesslock_id = null;
    }

    /**
     * Open session handler.
     *
     * {@see http://php.net/manual/en/function.session-set-save-handler.php}
     *
     * @param string $save_path
     * @param string $session_name
     * @return bool success
     */
    public function handler_open($save_path, $session_name) {
        // Note: we use the already open database.
        return (!empty($this->database));
    }

    /**
     * Close session handler.
     *
     * {@see http://php.net/manual/en/function.session-set-save-handler.php}
     *
     * @return bool success
     */
    public function handler_close() {
        if ($this->sesslock_id) {
            try {
                $this->release_session_lock();
            } catch (\Exception $ex) {
                // Ignore any problems.
            }
        }

        $this->sessdata_id = null;
        $this->sesslock_id = null;
        $this->lasthash = null;
        $this->timemodified = null;

        # DONE: Should we dispose of the MongoClient, MongoCollection handles, 
        # or just let the usual destructor cleanup handle that?
        # Probably leaving them as is plays nicer with persistent connections.
        # Also the mongodb cachestore driver doesn't seem to clean them up at 
        # all.
 
        return true;
    }

    /**
     * Read session handler.
     *
     * {@see http://php.net/manual/en/function.session-set-save-handler.php}
     *
     * @param string $sid
     * @return string
     */
    public function handler_read($sid) {
        try {
            if (!$record = $this->sessdata_collection->findOne(array('sid'=>$sid), array('timemodified'))) {
                // Let's cheat and skip locking if this is the first access,
                // do not create the record here, let the manager do it after session init.
                $this->failed = false;
                $this->sessdata_id = null;
                $this->sesslock_id = null;
                $this->timemodified = null;
                $this->lasthash = sha1('');
                return '';
            }
            $record_id = (string)$record['_id'];
            if ($this->sessdata_id and $this->sessdata_id != $record_id) {
                error_log('Second session read with different record id detected, cannot read session');
                $this->failed = true;
                $this->sessdata_id = null;
                $this->sesslock_id = null;
                $this->timemodified = null;
                return '';
            }
            if (!$this->sessdata_id) {
                // Lock session if exists and not already locked.
                // TODO: What about the "not already locked" part?
                $this->get_session_lock($sid);
                $this->sessdata_id = $record_id;
                $this->timemodified = $record['timemodified']->sec;
            }
        } catch (\dml_sessionwait_exception $ex) {
            // This is a fatal error, better inform users.
            // It should not happen very often - all pages that need long time to execute
            // should close session immediately after access control checks.
            error_log('Cannot obtain session lock for sid: '.$sid);
            $this->failed = true;
            throw $ex;

        } catch (\Exception $ex) {
            // Do not rethrow exceptions here, this should not happen.
            error_log('Unknown exception when starting mongodb session : '.$sid.' - '.$ex->getMessage());
            $this->failed = true;
            $this->sessdata_id = null;
            $this->sesslock_id = null;
            $this->timemodified = null;
            return '';
        }

        // Finally read the full session data because we know we have the lock now.
        if (!$record = $this->sessdata_collection->findOne(array('_id'=>$record['_id']), array('sessdata'))) {
            // Ignore - something else just deleted the session record.
            $this->failed = true;
            $this->sessdata_id = null;
            $this->sesslock_id = null;
            $this->timemodified = null;
            return '';
        }
        $this->failed = false;

        if (is_null($record['sessdata'])) {
            $data = '';
            $this->lasthash = sha1('');
        } else {
            // See NOTEs below.
            if ($this->session_serializer == 'igbinary') {
                $data = $record['sessdata']->bin;
            }
            else {
                # See NOTEs below.
                #$data = base64_decode($record['sessdata']);
                $data = $record['sessdata'];
            }

            # Hash on the data fed to and given from the user.
            $this->lasthash = sha1($data);
        }

        return $data;
    }

    /**
     * Write session handler.
     *
     * {@see http://php.net/manual/en/function.session-set-save-handler.php}
     *
     * NOTE: Do not write to output or throw any exceptions!
     *       Hopefully the next page is going to display nice error or it recovers...
     *
     * @param string $sid
     * @param string $session_data
     * @return bool success
     */
    public function handler_write($sid, $session_data) {
        if ($this->failed) {
            // Do not write anything back - we failed to start the session properly.
            return false;
        }

        # Hash on the data fed to and given from the user.
        $hash = sha1($session_data);
        # Try to skip a write if we can.
        # TODO: Hmm, will this interact poorly with the timemodified pruning behavior?
        # Probably not since there's a lot of _lastaccess and _lastloaded type 
        # of fields in the session data that cause this hash to never remain 
        # the same so it gets updated everytime anyways.
        if ($hash === $this->lasthash) {
            return true;
        }

        // Try sending raw binary data (for use with igbinary).
        // NOTE: Requires changing the sessions.sessdata column schema to LONGBLOB instead of LONGTEXT.
        if ($this->session_serializer == 'igbinary') {
            $sessdata = new \MongoBinData($session_data, \MongoBinData::BYTE_ARRAY);
        }
        else {
            #$sessdata = base64_encode($session_data); // There might be some binary mess :-(
            $sessdata = $session_data;  // but let's try it without for now
        }

        try {
            # Construct a document to update or insert into the db.
            # NOTE: The save() mongo function is a little noisy in the logs, so 
            # we try and deal with the right method ourselves manually.
            $upsert_doc = array(
                'sessdata' => $sessdata
            );
            $options = array(
                'safe' => $this->usesafe,
                'w' => ($this->usesafe) ? 1 : 0
            );
            if ($this->sessdata_id) {   # update
                // Only update the timemodified field periodically to reduce index fixups.
                // See Also: manager.php
                $updatefreq = empty($CFG->session_update_timemodified_frequency) ? 20 : $CFG->session_update_timemodified_frequency;
                if ($this->timemodified < time() - $updatefreq) {
                    // Update the session modified flag only once every 20 seconds.
                    $upsert_doc['timemodified'] = new \MongoDate(time());
                }

                $doc_id = new \MongoId($this->sessdata_id);
                $result = $this->sessdata_collection->update(
                    array(
                        '_id' => $doc_id,
                        'sid' => $sid
                    ),
                    array('$set' => $upsert_doc),
                    $options
                );
                if (!self::check_mongodb_response($result)) {
                    error_log(print_r($result, true));
                    throw new exception('mongodb-update-problem', 'error');
                }
            } else {    # insert
                // This happens in the first request when session record was just created in manager.
                $upsert_doc['_id'] = new \MongoId();
                $upsert_doc['sid'] = $sid;
                $upsert_doc['timemodified'] = new \MongoDate(time());
                $result = $this->sessdata_collection->insert($upsert_doc, $options);
                if (!self::check_mongodb_response($result)) {
                    error_log(print_r($result, true));
                    throw new exception('mongodb-insert-problem', 'error');
                }

                # Save the sessdata document id.
                # NOTE: This should be generated client side, so it should work, even with usesafe=false.
                # http://www.php.net/manual/en/class.mongoid.php
                $this->sessdata_id = (string)$upsert_doc['_id'];
            }
        } catch (\MongoException $ex) {
            error_log('MongoException when writing session data : '.$sid.' - '.$ex->getMessage());
        } catch (\Exception $ex) {
            // Do not rethrow exceptions here, this should not happen.
            error_log('Unknown exception when writing MongoDB session data : '.$sid.' - '.$ex->getMessage());
        }

        return true;
    }

    /**
     * Destroy session handler.
     *
     * {@see http://php.net/manual/en/function.session-set-save-handler.php}
     *
     * @param string $sid
     * @return bool success
     */
    public function handler_destroy($sid) {
        if (!$session = $this->sessdata_collection->findOne(array('sid'=>$sid), array('sid'))) {
            if ($sid == session_id()) {
                $this->sessdata_id = null;
                $this->sesslock_id = null;
                $this->timemodified = null;
                $this->lasthash = null;
            }
            return true;
        }

        $session_id = (string)$session['_id'];
        if ($this->sessdata_id and $session_id == $this->sessdata_id) {
            if ($this->sesslock_id) {
                try {
                    $this->release_session_lock();
                } catch (\Exception $ex) {
                    // Ignore problems.
                }
            }
            $this->sessdata_id = null;
            $this->sesslock_id = null;
            $this->timemodified = null;
            $this->lasthash = null;
        }

        # Remove by $sid per session_set_save_handler() spec.
        #$result = $this->sessdata_collection->remove(array('_id'=>$session['id']), array(
        $result = $this->sessdata_collection->remove(array('sid'=>$sid), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-remove-problem', 'error');
        }

        # Just for safety's sake remove all sesslocks for that sid?
        $result = $this->sesslock_collection->remove(array('sid'=>$sid), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) {
            error_log(print_r($result, true));
            throw new exception('mongodb-remove-problem', 'error');
        }

        return true;
    }

    /**
     * GC session handler.
     *
     * {@see http://php.net/manual/en/function.session-set-save-handler.php}
     *
     * @param int $ignored_maxlifetime moodle uses special timeout rules
     * @return bool success
     */
    public function handler_gc($ignored_maxlifetime) {
        // This should do something only if cron is not running properly...
        if (!$stalelifetime = ini_get('session.gc_maxlifetime')) {
            return true;
        }
        $purgebefore = new \MongoDate(time() - $stalelifetime);
        $cursor = $this->sessdata_collection->find(array('timemodified' => array('$lt', $purgebefore)), array('sid'));
        foreach ($cursor as $doc) {
            $result = $this->sessdata_collection->remove(array('sid' => $doc['sid']), array(
                'safe' => $this->usesafe,
                'w' => ($this->usesafe) ? 1 : 0,
            ));
            if (!self::check_mongodb_response($result)) {
                error_log(print_r($result, true));
                throw new exception('mongodb-remove-problem', 'error');
            }

            $result = $this->sesslock_collection->remove(array('sid' => $doc['sid']), array(
                'safe' => $this->usesafe,
                'w' => ($this->usesafe) ? 1 : 0,
            ));
            if (!self::check_mongodb_response($result)) {
                error_log(print_r($result, true));
                throw new exception('mongodb-remove-problem', 'error');
            }
        }
        return true;
    }
}
