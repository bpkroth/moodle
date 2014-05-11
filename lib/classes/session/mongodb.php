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
            // Mongo class is deprecated.
            $this->connection = new MongoClient($server, $options);
        } catch (MongoConnectionException $e) {
            // We only want to catch MongoConnectionExceptions here.
        }
        $this->database = $this->connection->selectDB($databasename);
        $this->sessdata_collection = $this->database->selectCollection('sessdata');
        $this->sesslock_collection = $this->database->selectCollection('sesslock');
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
     */
    public function setup_collection_indexes() {
        global $CFG;

        # Check to see if we need to reindex.
 
        $reindex_timestamp = 0; # by default don't reindex
        if (!empty($CFG->session_mongodb_reindex_timestamp)) {
            $reindex_timestamp = (int)$CFG->session_mongodb_reindex_timestamp;
        }

        $last_reindex_timestamp = null; # but assume we need to if we can't tell when it was last done
        $config_collection = $this->database->selectCollection('config');
        $result = $config_collection->ensureIndex(array('key' => 1), array(
            'safe' => $this->usesafe,
            'w' => ($this->usesafe) ? 1 : 0,
            'unique' => true,
            'name' => 'idx_key_unique'
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        $config_doc = $config_collection->findOne(array('key' => 'last_reindex_timestamp'), array('value'));
        if (!empty($config_doc) && !empty($config_doc['value'])) {
            $last_reindex_timestamp = $config_doc['value'];
        }
        if (!empty($last_reindex_timestamp) && $last_reindex_timestamp >= $reindex_timestamp) {
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
        # prone to contention.

        # Setup the sessdata collection indices.
        $result = $this->sessdata_collection->deleteIndexes();
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        # Here's an index to enforce uniqueness constraints on the sid field.
        $result = $this->sessdata_collection->ensureIndex(array('sid' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'unique' => true,
            'dropDups' => true,
            'name' => 'idx_sid_unique'
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        # We should only ever need to perform point lookups of session ids, so 
        # hash indexes should be most efficient, so add one of those too.
        $result = $this->sessdata_collection->ensureIndex(array('sid' => 'hashed'), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            #'unique' => true,  # hash indexes currently don't support unique constraints
            'name' => 'idx_sid_hashed'
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        $result = $this->sessdata_collection->ensureIndex(array('timemodified' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'expireAfterSeconds' => $this->sessdata_expire,
            'name' => 'idx_timemodified'
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

        # Setup the sesslock collection.
        $result = $this->sesslock_collection->deleteIndexes();
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        # Here's an index to enforce uniqueness constraints on the sid field.
        $result = $this->sesslock_collection->ensureIndex(array('sid' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'unique' => true,
            'dropDups' => true,
            'name' => 'idx_sid_unique'
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        # We should only ever need to perform point lookups of session ids, so 
        # hash indexes should be most efficient, so add one of those too.
        $result = $this->sesslock_collection->ensureIndex(array('sid' => 'hashed'), array(
            #'unique' => true,  # hash indexes currently don't support unique constraints
            'name' => 'idx_sid_hashed'
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        $result = $this->sesslock_collection->ensureIndex(array('timemodified' => 1), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
            'expireAfterSeconds' => $this->lock_expire,
            'name' => 'idx_timemodified'
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

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
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
    }

    /**
     * Init session handler.
     */
    public function init() {
        # DONE: We may want to move all of these ensureIndex() collection setup 
        # calls to something that's a bit more periodic, or perhaps done by the 
        # administrator prior to using this session store.
        $this->setup_collections_indices();

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
            if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

            $result = $this->sesslock_collection->remove();
            if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        } catch (MongoException $ignored) {
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
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

        $result = $this->sesslock_collection->remove(array('sid'=>$sid), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

        return;
    }

    /**
     * A function to acquire a lock document.
     */
    private function get_session_lock($sid) {
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
                # to check the response.
                #'safe' => $this->usesafe,
                #'w' => ($this->usesafe) ? 1 : 0,
            ));
            if (!self::check_mongodb_response($result)) {
                # if it fails, check to see if we've timedout
                if (time() - $start_time >= $this->acquiretimeout) {
                    break;
                }
                # else, wait a moment and try again
                usleep(2000);  # 2 ms
            }
            else {
                // lock acquired - save the id
                $this->sesslock_id = (string)$lockdoc['_id'];
            }
        } while (empty($this->sesslock_id));

        if (empty($this->sesslock_id)) {
            throw new dml_sessionwait_exception();
        }
    }

    /**
     * A function to release a lock document.
     */
    private function release_session_lock() {
        if (empty($this->sesslock_id)) {
            throw new coding_exception('sesslock_id is not set.');
        }
        $lockdoc = array(
            '_id' => new MongoId($this->sesslock_id)
        );
        $result = $this->sesslock_collection->remove($lockdoc, array(
            # These locks aren't removed on disconnect, so we need to try a 
            # little harder to make sure they get released.
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        if ($result['n'] == 0) throw new exception('mongodbwriteproblem', 'error');    # perhaps something else released the lock
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
        return true;
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
            #if (!$this->sesslock_id) {
            if (!$this->sessdata_id) {
                // Lock session if exists and not already locked.
                $this->get_session_lock($sid);
                $this->sessdata_id = $record_id;
                $this->timemodified = $record['timemodified'];
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
        if (!$record = $this->sessdata_collection->findOne(array('_id'=>$record['_id']), 'sessdata')) {
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
                #$data = base64_decode($record['sessdata']);
                $data = $record['sessdata'];
            }
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

        $hash = sha1($session_data);
        // Try sending raw binary data (for use with igbinary).
        // NOTE: Requires changing the sessions.sessdata column schema to LONGBLOB instead of LONGTEXT.
        if ($this->session_serializer == 'igbinary') {
            $sessdata = new MongoBinData($session_data);
        }
        else {
            #$sessdata = base64_encode($session_data); // There might be some binary mess :-(
            $sessdata = $session_data;  // but let's try it without for now
        }

        if ($hash === $this->lasthash) {
            return true;
        }

        try {
            # Construct a document to update or insert into the db.
            $upsert = array(
                'sessdata' => $sessdata
            );
            if ($this->sessdata_id) {
                $upsert['_id'] = new MongoId($this->sessdata_id);

                // Only update the timemodified field periodically to reduce index fixups.
                // See Also: manager.php
                $updatefreq = empty($CFG->session_update_timemodified_frequency) ? 20 : $CFG->session_update_timemodified_frequency;
                if ($this->timemodified < time() - $updatefreq) {
                    // Update the session modified flag only once every 20 seconds.
                    $upsert['timemodified'] = time();
                }
            } else {
                // This happens in the first request when session record was just created in manager.
                $upsert['sid'] = $sid;
                $upsert['timemodified'] = time();
            }

            $result = $this->sessdata_collection->save($upsert, array(
                'safe' => $this->usesafe,
                'w' => ($this->usesafe) ? 1 : 0,
            ));
            if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

            # Save the sessdata document id.
            # NOTE: This should be generated client side, so it should work, even with usesafe=false.
            # http://www.php.net/manual/en/class.mongoid.php
            $this->sessdata_id = (string)$upsert['_id'];
        } catch (MongoException $ex) {
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
            try {
                $this->database->release_session_lock();
            } catch (\Exception $ex) {
                // Ignore problems.
            }
            $this->sessdata_id = null;
            $this->sesslock_id = null;
            $this->timemodified = null;
            $this->lasthash = null;
        }

        #$result = $this->sessdata_collection->remove(array('_id'=>$session['id']), array(
        $result = $this->sessdata_collection->remove(array('sid'=>$sid), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

        $result = $this->sesslock_collection->remove(array('sid'=>$sid), array(
            #'safe' => $this->usesafe,
            #'w' => ($this->usesafe) ? 1 : 0,
        ));
        if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

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
        $purgebefore = (time() - $stalelifetime);
        $cursor = $this->sessdata_collection->find(array('timemodified' => array('$lt', $purgebefore)), array('sid'));
        foreach ($cursor as $doc) {
            $result = $this->sessdata_collection->remove(array('sid' => $doc['sid']), array(
                'safe' => $this->usesafe,
                'w' => ($this->usesafe) ? 1 : 0,
            ));
            if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');

            $result = $this->sesslock_collection->remove(array('sid' => $doc['sid']), array(
                'safe' => $this->usesafe,
                'w' => ($this->usesafe) ? 1 : 0,
            ));
            if (!self::check_mongodb_response($result)) throw new exception('mongodbwriteproblem', 'error');
        }
        return true;
    }
}
