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
 * Separate Database based session handler.
 *
 * The normal session database driver stores all of the sessdata in the 
 * database.  Since that database may have high durability concerns that means 
 * that every page must wait on a database commit and therefore flush to disk 
 * of transaction log and possibly replication log before returning.  This can 
 * be an enormous amount of churn and latency.
 *
 * Using a separate database for that data allows one to configure a database 
 * with somewhat relaxed durability requirements (eg: no flush at commit and/or 
 * no binary logging).
 *
 * This is required in the case of MySQL, for instance, since it doesn't have a 
 * nice way to specify relaxed durability on a per transaction basis like MSSQL 
 * 2014 and Oracle does.  MongoDB's write concerns would be another strategy.
 *
 * Using something like Memcached is the other obvious answer, but that
 * 1) doesn't really support locking
 * 2) has no persistence (by default - there are implementations that do)
 * 3) has no access control (any client allowed to connect can flush the contents, 
 * effectively logging all users out)
 *
 *
 * TODO: Add whatever Moodle specific stuff that's required to ensure that the 
 * tables are setup correctly automatically.
 *
 * For now, I've just used the following table schema:
 * 
CREATE TABLE `mdl_sessdata` (
  `id` bigint(10) NOT NULL AUTO_INCREMENT,
  `sid` varchar(128) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `sessdata` longblob COMMENT 'Use a blob so we can support alternative serializers',
  `timemodified` bigint(10) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `mdl_sess_sid_uix` (`sid`),
  KEY `mdl_sess_tim_ix` (`timemodified`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='Separate database based session storage'
 *
 * NOTE: ROW_FORMAT=DYNAMIC is a MySQL 5.6 feature for dynamically determining 
 * when to store the sessdata blob in the clustered btree or not.  When it's 
 * too large, as the Moodle sessdata often is, it makes more sense to keep it 
 * in a separate datapage so that the btree on sid can be more effecient.
 * 
 * NOTE: We could have probably used the same table format as mdl_sessions and
 * then extended the core\sessions\database class, but I think it would have 
 * meant too many changes to be of much use and I liked this clean break a 
 * little bit better.
 *
 *
 * NOTE: We've also included igbinary support patches from my 
 * patches/igbinary-sessions-serializer branch.
 *
 *
 * @package    core
 * @copyright  2014 Brian Kroth {@link http://cae.wisc.edu/~bpkroth}
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

namespace core\session;

defined('MOODLE_INTERNAL') || die();

/**
 * Separate Database based session handler.
 *
 * See above for NOTEs.
 *
 * @package    core
 * @copyright  2014 Brian Kroth {@link http://cae.wisc.edu/~bpkroth}
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */
class sessions_db extends handler {
    /** @var \stdClass $record session record id */
    protected $recordid = null;

    /** @var \stdClass $record session timemodified */
    protected $timemodified = null;

    /** @var \moodle_database $database session database */
    protected $database = null;

    /** @var bool $failed session read/init failed, do not write back to DB */
    protected $failed = false;

    /** @var string $lasthash hash of the session data content */
    protected $lasthash = null;

    /** @var int $acquiretimeout how long to wait for session lock */
    protected $acquiretimeout = 120;

    /** @var string session_serializer that was requested */
    protected $session_serializer = 'php';

    /**
     * Create new instance of handler.
     */
    public function __construct() {
        global $SDB, $CFG;
        setup_SDB();    // See: dmllib.php
        // Note: we store the reference here because we need to modify database in shutdown handler.
        $this->database = $SDB;

        if (!empty($CFG->session_database_acquire_lock_timeout)) {
            $this->acquiretimeout = (int)$CFG->session_database_acquire_lock_timeout;
        }

        if (isset($CFG->session_serializer)) {
            $this->session_serializer = $CFG->session_serializer;
        }
    }

    /**
     * Init session handler.
     */
    public function init() {
        if (!$this->database->session_lock_supported()) {
            throw new exception('sessionhandlerproblem', 'error', '', null, 'Database does not support session locking');
        }

        $result = session_set_save_handler(array($this, 'handler_open'),
            array($this, 'handler_close'),
            array($this, 'handler_read'),
            array($this, 'handler_write'),
            array($this, 'handler_destroy'),
            array($this, 'handler_gc'));
        if (!$result) {
            throw new exception('dbsessionhandlerproblem', 'error');
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
        try {
            return $this->database->record_exists('sessdata', array('sid'=>$sid));
        } catch (\dml_exception $ex) {
            return false;
        }
    }

    /**
     * Kill all active sessions, the core sessions table is
     * purged afterwards.
     */
    public function kill_all_sessions() {
        try {
            $this->database->delete_records('sessdata');
        } catch (\dml_exception $ignored) {
            // Do not show any warnings - might be during upgrade/installation.
        }
        return;
    }

    /**
     * Kill one session, the session record is removed afterwards.
     * @param string $sid
     */
    public function kill_session($sid) {
        $this->database->delete_records('sessdata', array('sid'=>$sid));
        return;
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
        if ($this->recordid) {
            try {
                $this->database->release_session_lock($this->recordid);
            } catch (\Exception $ex) {
                // Ignore any problems.
            }
        }
        $this->recordid = null;
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
            if (!$record = $this->database->get_record('sessdata', array('sid'=>$sid), 'id, timemodified')) {
                // Let's cheat and skip locking if this is the first access,
                // do not create the record here, let the manager do it after session init.
                $this->failed = false;
                $this->recordid = null;
                $this->timemodified = null;
                $this->lasthash = sha1('');
                return '';
            }
            if ($this->recordid and $this->recordid != $record->id) {
                error_log('Second session read with different record id detected, cannot read session');
                $this->failed = true;
                $this->recordid = null;
                $this->timemodified = null;
                return '';
            }
            if (!$this->recordid) {
                // Lock session if exists and not already locked.
                $this->database->get_session_lock($record->id, $this->acquiretimeout);
                $this->recordid = $record->id;
                $this->timemodified = $record->timemodified;
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
            error_log('Unknown exception when starting database session : '.$sid.' - '.$ex->getMessage());
            $this->failed = true;
            $this->recordid = null;
            $this->timemodified = null;
            return '';
        }

        // Finally read the full session data because we know we have the lock now.
        if (!$record = $this->database->get_record('sessdata', array('id'=>$record->id), 'id, sessdata')) {
            // Ignore - something else just deleted the session record.
            $this->failed = true;
            $this->recordid = null;
            $this->timemodified = null;
            return '';
        }
        $this->failed = false;

        if (is_null($record->sessdata)) {
            $data = '';
            $this->lasthash = sha1('');
        } else {
            // See NOTEs below.
            if ($this->session_serializer == 'igbinary') {
                $data = $record->sessdata;
            }
            else {
                #$data = base64_decode($record->sessdata);
                $data = $record->sessdata;
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
        // NOTE: igbinary encoded session data doesn't compress very well (See Also: patches/mysqli-client-compression branch).
        if ($this->session_serializer == 'igbinary') {
            $sessdata = $session_data;
        }
        else {
            #$sessdata = base64_encode($session_data); // There might be some binary mess :-(
            $sessdata = $session_data;  // But we don't really care since we're using a LONGBLOB field anyways.
        }

        if ($hash === $this->lasthash) {
            return true;
        }

        try {
            $upsert = new \stdClass();
            $upsert->sessdata = $sessdata;
            if ($this->recordid) {
                $upsert->id = $this->recordid;

                // Only update the timemodified field periodically to reduce index fixups.
                // See Also: manager.php
                global $CFG;
                $updatefreq = empty($CFG->session_update_timemodified_frequency) ? 20 : $CFG->session_update_timemodified_frequency;
                if ($this->timemodified < time() - $updatefreq) {
                    // Update the session modified flag only once every 20 seconds.
                    $upsert->timemodified = time();
                }

                $this->database->update_record('sessdata', $upsert);
            } else {
                // This happens in the first request when session record was just created in manager.
                $upsert->sid = $sid;
                $upsert->timemodified = time();
                $this->recordid = $this->database->insert_record('sessdata', $upsert);
            }
        } catch (\Exception $ex) {
            // Do not rethrow exceptions here, this should not happen.
            error_log('Unknown exception when writing database session data : '.$sid.' - '.$ex->getMessage());
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
        if (!$session = $this->database->get_record('sessdata', array('sid'=>$sid), 'id, sid')) {
            if ($sid == session_id()) {
                $this->recordid = null;
                $this->timemodified = null;
                $this->lasthash = null;
            }
            return true;
        }

        if ($this->recordid and $session->id == $this->recordid) {
            try {
                $this->database->release_session_lock($this->recordid);
            } catch (\Exception $ex) {
                // Ignore problems.
            }
            $this->recordid = null;
            $this->timemodified = null;
            $this->lasthash = null;
        }

        $this->database->delete_records('sessdata', array('id'=>$session->id));

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
        $params = array('purgebefore' => (time() - $stalelifetime));
        $this->database->delete_records_select('sessdata', 'timemodified < :purgebefore', $params);
        return true;
    }
}
