/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _HOTSTUFF_CONSENSUS_H
#define _HOTSTUFF_CONSENSUS_H

#include <cassert>
#include <set>
#include <unordered_map>

#include "hotstuff/promise.hpp"
#include "hotstuff/type.h"
#include "hotstuff/entity.h"
#include "hotstuff/crypto.h"

namespace hotstuff {

struct Proposal;
struct Prepare;
struct Commit1;
struct Collect;
struct Csend;
struct Echo;
struct Ready;

struct Finality;

/** Abstraction for HotStuff protocol state machine (without network implementation). */
class HotStuffCore {
    block_t b0;                                  /** the genesis block */
    /* === state variables === */
    /** block containing the QC for the highest block having one */
    std::pair<block_t, quorum_cert_bt> hqc;   /**< highest QC */
    block_t b_lock;                            /**< locked block */
    block_t b_exec;                            /**< last executed block */
    uint32_t vheight;          /**< height of the block last prepared for */
    /* === auxilliary variables === */
    privkey_bt priv_key;            /**< private key for signing votes */
    std::set<block_t> tails;   /**< set of tail blocks */
    ReplicaConfig config;                   /**< replica configuration */
    /* === async event queues === */
    std::unordered_map<block_t, promise_t> qc_waiting;
    promise_t propose_waiting;
    promise_t receive_proposal_waiting;
    promise_t hqc_update_waiting;
    /* == feature switches == */
    /** always vote negatively, useful for some PaceMakers */
    bool vote_disabled;

    bool sent_prepare;
    int sent_commit = -1;
    int vpbp = -1;
    int vcbc = -1;
    int vstar = -1;
    int pstar = -1;


    std::set<int> esent;
    std::set<int> rsent;


    std::set<uint32_t> issued_blocks;



    block_t get_delivered_blk(const uint256_t &blk_hash);
    void sanity_check_delivered(const block_t &blk);
    void update(const block_t &nblk);

    void on_commit(const block_t &nblk);

    void update_hqc(const block_t &_hqc, const quorum_cert_bt &qc);
    void on_hqc_update();
    void on_qc_finish(const block_t &blk);
    void on_propose_(const Proposal &prop);
    void on_receive_proposal_(const Proposal &prop);

    protected:
    ReplicaID id;                  /**< identity of the replica itself */

    public:
    BoxObj<EntityStorage> storage;

    HotStuffCore(ReplicaID id, privkey_bt &&priv_key);
    virtual ~HotStuffCore() {
        b0->qc_ref = nullptr;
    }

    /* Inputs of the state machine triggered by external events, should called
     * by the class user, with proper invariants. */

    /** Call to initialize the protocol, should be called once before all other
     * functions. */
    void on_init(uint32_t nfaulty);

    /* TODO: better name for "delivery" ? */
    /** Call to inform the state machine that a block is ready to be handled.
     * A block is only delivered if itself is fetched, the block for the
     * contained qc is fetched and all parents are delivered. The user should
     * always ensure this invariant. The invalid blocks will be dropped by this
     * function.
     * @return true if valid */
    bool on_deliver_blk(const block_t &blk);

    /** Call upon the delivery of a proposal message.
     * The block mentioned in the message should be already delivered. */
    void on_receive_proposal(const Proposal &prop);

    /** Call upon the delivery of a vote message.
     * The block mentioned in the message should be already delivered. */
    void on_receive_prepare(const Prepare &vote);
    void on_receive_commit1(const Commit1 &vote);
    void on_receive_collect(const Collect &vote);
    void on_receive_csend(const Csend &vote);
    void on_receive_echo(const Echo &vote);
    void on_receive_ready(const Ready &vote);

    void on_send_collect(const Finality &vote);



    /** Call to submit new commands to be decided (executed). "Parents" must
     * contain at least one block, and the first block is the actual parent,
     * while the others are uncles/aunts */
    block_t on_new_view(const std::vector<uint256_t> &cmds, const std::vector<int> &keys, const std::vector<int> &vals,int prop_id, int sid,
                    const std::vector<block_t> &parents, bytearray_t &&extra = bytearray_t());

    /* Functions required to construct concrete instances for abstract classes.
     * */

    /* Outputs of the state machine triggering external events.  The virtual
     * functions should be implemented by the user to specify the behavior upon
     * the events. */
    protected:
    /** Called by HotStuffCore upon the decision being made for cmd. */
    virtual void do_decide(Finality &&fin) = 0;
    virtual void do_consensus(const block_t &blk) = 0;
    /** Called by HotStuffCore upon broadcasting a new proposal.
     * The user should send the proposal message to all replicas except for
     * itself. */
    virtual void do_broadcast_proposal(const Proposal &prop) = 0;
    /** Called upon sending out a new vote to the next proposer.  The user
     * should send the vote message to a *good* proposer to have good liveness,
     * while safety is always guaranteed by HotStuffCore. */
    virtual void send_prepare(ReplicaID last_proposer, const Prepare &vote) = 0;
    virtual void send_commit1(ReplicaID last_proposer, const Commit1 &vote) = 0;
    virtual void send_collect(ReplicaID last_proposer, const Collect &vote) = 0;
    virtual void send_csend(ReplicaID last_proposer, const Csend &vote) = 0;
    virtual void send_echo(ReplicaID last_proposer, const Echo &vote) = 0;
    virtual void send_ready(ReplicaID last_proposer, const Ready &vote) = 0;

    virtual int get_part_decided() = 0;


//    virtual void start_remote_view_change_timer(int timer_cid, const block_t base) = 0;


    /* The user plugs in the detailed instances for those
     * polymorphic data types. */
    public:
    /** Create a partial certificate that proves the vote for a block. */
    virtual part_cert_bt create_part_cert(const PrivKey &priv_key, const uint256_t &blk_hash) = 0;
    /** Create a partial certificate from its seralized form. */
    virtual part_cert_bt parse_part_cert(DataStream &s) = 0;
    /** Create a quorum certificate that proves 2f+1 votes for a block. */
    virtual quorum_cert_bt create_quorum_cert(const uint256_t &blk_hash) = 0;
    /** Create a quorum certificate from its serialized form. */
    virtual quorum_cert_bt parse_quorum_cert(DataStream &s) = 0;
    /** Create a command object from its serialized form. */
    //virtual command_t parse_cmd(DataStream &s) = 0;

    public:
    /** Add a replica to the current configuration. This should only be called
     * before running HotStuffCore protocol. */
    void add_replica(ReplicaID rid, const PeerId &peer_id, pubkey_bt &&pub_key);
    /** Try to prune blocks lower than last committed height - staleness. */
    void prune(uint32_t staleness);

    /* PaceMaker can use these functions to monitor the core protocol state
     * transition */
    /** Get a promise resolved when the block gets a QC. */
    promise_t async_qc_finish(const block_t &blk);
    /** Get a promise resolved when a new block is proposed. */
    promise_t async_wait_proposal();
    /** Get a promise resolved when a new proposal is received. */
    promise_t async_wait_receive_proposal();
    /** Get a promise resolved when hqc is updated. */
    promise_t async_hqc_update();

    /* Other useful functions */
    const block_t &get_genesis() const { return b0; }
    const block_t &get_hqc() { return hqc.first; }
    const ReplicaConfig &get_config() const { return config; }
    ReplicaID get_id() const { return id; }
    const std::set<block_t> get_tails() const { return tails; }
    operator std::string () const;
    void set_vote_disabled(bool f) { vote_disabled = f; }
};

/** Abstraction for proposal messages. */
struct Proposal: public Serializable {
    ReplicaID proposer;
    /** block being proposed */
    block_t blk;
    /** handle of the core object to allow polymorphism. The user should use
     * a pointer to the object of the class derived from HotStuffCore */
    HotStuffCore *hsc;

    Proposal(): blk(nullptr), hsc(nullptr) {}
    Proposal(ReplicaID proposer,
            const block_t &blk,
            HotStuffCore *hsc):
        proposer(proposer),
        blk(blk), hsc(hsc) {}

    void serialize(DataStream &s) const override {
        s << proposer
          << *blk;
    }

    void unserialize(DataStream &s) override {
        assert(hsc != nullptr);
        s >> proposer;
        Block _blk;
        _blk.unserialize(s, hsc);
        blk = hsc->storage->add_blk(std::move(_blk), hsc->get_config());
    }

    operator std::string () const {
        DataStream s;
        s << "<proposal "
          << "rid=" << std::to_string(proposer) << " "
          << "blk=" << get_hex10(blk->get_hash()) << ">";
        return s;
    }
};

/** Abstraction for vote messages. */
struct Prepare: public Serializable {
    ReplicaID voter;
    /** block being prepared */
    uint256_t blk_hash;
    /** proof of validity for the vote */
    part_cert_bt cert;
    
    /** handle of the core object to allow polymorphism */
    HotStuffCore *hsc;

    Prepare(): cert(nullptr), hsc(nullptr) {}
    Prepare(ReplicaID voter,
        const uint256_t &blk_hash,
        part_cert_bt &&cert,
        HotStuffCore *hsc):
        voter(voter),
        blk_hash(blk_hash),
        cert(std::move(cert)), hsc(hsc) {}

    Prepare(const Prepare &other):
        voter(other.voter),
        blk_hash(other.blk_hash),
        cert(other.cert ? other.cert->clone() : nullptr),
        hsc(other.hsc) {}

    Prepare(Prepare &&other) = default;
    
    void serialize(DataStream &s) const override {
        s << voter << blk_hash << *cert;
    }

    void unserialize(DataStream &s) override {
        assert(hsc != nullptr);
        s >> voter >> blk_hash;
        cert = hsc->parse_part_cert(s);
    }

    bool verify() const {
        assert(hsc != nullptr);
        return cert->verify(hsc->get_config().get_pubkey(voter)) &&
                cert->get_obj_hash() == blk_hash;
    }

    promise_t verify(VeriPool &vpool) const {
        assert(hsc != nullptr);
        return cert->verify(hsc->get_config().get_pubkey(voter), vpool).then([this](bool result) {
            return result && cert->get_obj_hash() == blk_hash;
        });
    }

    operator std::string () const {
        DataStream s;
        s << "<prepare "
          << "rid=" << std::to_string(voter) << " "
          << "blk=" << get_hex10(blk_hash) << ">";
        return s;
    }
};



/** Abstraction for vote messages. */
struct Commit1: public Serializable {
    ReplicaID voter;
    /** block being prepared */
    uint256_t blk_hash;
    /** proof of validity for the vote */
    part_cert_bt cert;

    /** handle of the core object to allow polymorphism */
    HotStuffCore *hsc;

    Commit1(): cert(nullptr), hsc(nullptr) {}
    Commit1(ReplicaID voter,
            const uint256_t &blk_hash,
            part_cert_bt &&cert,
            HotStuffCore *hsc):
            voter(voter),
            blk_hash(blk_hash),
            cert(std::move(cert)), hsc(hsc) {}

    Commit1(const Commit1 &other):
            voter(other.voter),
            blk_hash(other.blk_hash),
            cert(other.cert ? other.cert->clone() : nullptr),
            hsc(other.hsc) {}

    Commit1(Commit1 &&other) = default;

    void serialize(DataStream &s) const override {
        s << voter << blk_hash << *cert;
    }

    void unserialize(DataStream &s) override {
        assert(hsc != nullptr);
        s >> voter >> blk_hash;
        cert = hsc->parse_part_cert(s);
    }

    bool verify() const {
        assert(hsc != nullptr);
        return cert->verify(hsc->get_config().get_pubkey(voter)) &&
               cert->get_obj_hash() == blk_hash;
    }

    promise_t verify(VeriPool &vpool) const {
        assert(hsc != nullptr);
        return cert->verify(hsc->get_config().get_pubkey(voter), vpool).then([this](bool result) {
            return result && cert->get_obj_hash() == blk_hash;
        });
    }

    operator std::string () const {
        DataStream s;
        s << "<commit1 "
          << "rid=" << std::to_string(voter) << " "
          << "blk=" << get_hex10(blk_hash) << ">";
        return s;
    }
};


/** Abstraction for vote messages. */
    struct Collect: public Serializable {
        ReplicaID voter;
        uint256_t blk_hash;
        part_cert_bt cert;
        HotStuffCore *hsc;

        /** junk data to inflate message size */
        std::vector<uint8_t> junk_data;

        Collect(): cert(nullptr), hsc(nullptr), junk_data(20 * 1024, 'J') {}

        Collect(ReplicaID voter,
                const uint256_t &blk_hash,
                part_cert_bt &&cert,
                HotStuffCore *hsc,
                size_t junk_size = 20 * 1024):
                voter(voter),
                blk_hash(blk_hash),
                cert(std::move(cert)),
                hsc(hsc),
                junk_data(junk_size, 'J') {}

        Collect(const Collect &other):
                voter(other.voter),
                blk_hash(other.blk_hash),
                cert(other.cert ? other.cert->clone() : nullptr),
                hsc(other.hsc),
                junk_data(other.junk_data) {}

        Collect(Collect &&other) = default;

        void serialize(DataStream &s) const override {
            s << voter << blk_hash << *cert;
            s << (uint32_t)junk_data.size();
            for (auto b : junk_data)
                s << b;
        }

        void unserialize(DataStream &s) override {
            assert(hsc != nullptr);
            s >> voter >> blk_hash;
            cert = hsc->parse_part_cert(s);
            uint32_t size;
            s >> size;
            junk_data.resize(size);
            for (uint32_t i = 0; i < size; ++i)
                s >> junk_data[i];
        }

        bool verify() const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter)) &&
                   cert->get_obj_hash() == blk_hash;
        }

        promise_t verify(VeriPool &vpool) const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter), vpool).then([this](bool result) {
                return result && cert->get_obj_hash() == blk_hash;
            });
        }

        void set_junk(size_t size, uint8_t fill = 'J') {
            junk_data = std::vector<uint8_t>(size, fill);
        }

        operator std::string () const {
            DataStream s;
            s << "<Collect "
              << "rid=" << std::to_string(voter) << " "
              << "blk=" << get_hex10(blk_hash) << ">";
            return s;
        }
    };





/** Abstraction for vote messages. */
    struct Csend: public Serializable {
        ReplicaID voter;
        /** block being prepared */
        uint256_t blk_hash;
        /** proof of validity for the vote */
        part_cert_bt cert;

        /** handle of the core object to allow polymorphism */
        HotStuffCore *hsc;

        Csend(): cert(nullptr), hsc(nullptr) {}
        Csend(ReplicaID voter,
                const uint256_t &blk_hash,
                part_cert_bt &&cert,
                HotStuffCore *hsc):
                voter(voter),
                blk_hash(blk_hash),
                cert(std::move(cert)), hsc(hsc) {}

        Csend(const Csend &other):
                voter(other.voter),
                blk_hash(other.blk_hash),
                cert(other.cert ? other.cert->clone() : nullptr),
                hsc(other.hsc) {}

        Csend(Csend &&other) = default;

        void serialize(DataStream &s) const override {
            s << voter << blk_hash << *cert;
        }

        void unserialize(DataStream &s) override {
            assert(hsc != nullptr);
            s >> voter >> blk_hash;
            cert = hsc->parse_part_cert(s);
        }

        bool verify() const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter)) &&
                   cert->get_obj_hash() == blk_hash;
        }

        promise_t verify(VeriPool &vpool) const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter), vpool).then([this](bool result) {
                return result && cert->get_obj_hash() == blk_hash;
            });
        }

        operator std::string () const {
            DataStream s;
            s << "<Csend "
              << "rid=" << std::to_string(voter) << " "
              << "blk=" << get_hex10(blk_hash) << ">";
            return s;
        }
    };



/** Abstraction for vote messages. */
    struct Echo: public Serializable {
        ReplicaID voter;
        /** block being prepared */
        uint256_t blk_hash;
        /** proof of validity for the vote */
        part_cert_bt cert;

        /** handle of the core object to allow polymorphism */
        HotStuffCore *hsc;

        Echo(): cert(nullptr), hsc(nullptr) {}
        Echo(ReplicaID voter,
                const uint256_t &blk_hash,
                part_cert_bt &&cert,
                HotStuffCore *hsc):
                voter(voter),
                blk_hash(blk_hash),
                cert(std::move(cert)), hsc(hsc) {}

        Echo(const Echo &other):
                voter(other.voter),
                blk_hash(other.blk_hash),
                cert(other.cert ? other.cert->clone() : nullptr),
                hsc(other.hsc) {}

        Echo(Echo &&other) = default;

        void serialize(DataStream &s) const override {
            s << voter << blk_hash << *cert;
        }

        void unserialize(DataStream &s) override {
            assert(hsc != nullptr);
            s >> voter >> blk_hash;
            cert = hsc->parse_part_cert(s);
        }

        bool verify() const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter)) &&
                   cert->get_obj_hash() == blk_hash;
        }

        promise_t verify(VeriPool &vpool) const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter), vpool).then([this](bool result) {
                return result && cert->get_obj_hash() == blk_hash;
            });
        }

        operator std::string () const {
            DataStream s;
            s << "<Echo "
              << "rid=" << std::to_string(voter) << " "
              << "blk=" << get_hex10(blk_hash) << ">";
            return s;
        }
    };



/** Abstraction for vote messages. */
    struct Ready: public Serializable {
        ReplicaID voter;
        /** block being prepared */
        uint256_t blk_hash;
        /** proof of validity for the vote */
        part_cert_bt cert;

        /** handle of the core object to allow polymorphism */
        HotStuffCore *hsc;

        Ready(): cert(nullptr), hsc(nullptr) {}
        Ready(ReplicaID voter,
                const uint256_t &blk_hash,
                part_cert_bt &&cert,
                HotStuffCore *hsc):
                voter(voter),
                blk_hash(blk_hash),
                cert(std::move(cert)), hsc(hsc) {}

        Ready(const Ready &other):
                voter(other.voter),
                blk_hash(other.blk_hash),
                cert(other.cert ? other.cert->clone() : nullptr),
                hsc(other.hsc) {}

        Ready(Ready &&other) = default;

        void serialize(DataStream &s) const override {
            s << voter << blk_hash << *cert;
        }

        void unserialize(DataStream &s) override {
            assert(hsc != nullptr);
            s >> voter >> blk_hash;
            cert = hsc->parse_part_cert(s);
        }

        bool verify() const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter)) &&
                   cert->get_obj_hash() == blk_hash;
        }

        promise_t verify(VeriPool &vpool) const {
            assert(hsc != nullptr);
            return cert->verify(hsc->get_config().get_pubkey(voter), vpool).then([this](bool result) {
                return result && cert->get_obj_hash() == blk_hash;
            });
        }

        operator std::string () const {
            DataStream s;
            s << "<Ready "
              << "rid=" << std::to_string(voter) << " "
              << "blk=" << get_hex10(blk_hash) << ">";
            return s;
        }
    };





struct Finality: public Serializable {
    ReplicaID rid;
    int8_t decision;
    uint32_t cmd_idx;
    uint32_t cmd_height;
    uint256_t cmd_hash;
    uint256_t blk_hash;
    
    public:
    Finality() = default;
    Finality(ReplicaID rid,
            int8_t decision,
            uint32_t cmd_idx,
            uint32_t cmd_height,
            uint256_t cmd_hash,
            uint256_t blk_hash):
        rid(rid), decision(decision),
        cmd_idx(cmd_idx), cmd_height(cmd_height),
        cmd_hash(cmd_hash), blk_hash(blk_hash) {}

    void serialize(DataStream &s) const override {
        s << rid << decision
          << cmd_idx << cmd_height
          << cmd_hash;
        if (decision == 1) s << blk_hash;
    }

    void unserialize(DataStream &s) override {
        s >> rid >> decision
          >> cmd_idx >> cmd_height
          >> cmd_hash;
        if (decision == 1) s >> blk_hash;
    }

    operator std::string () const {
        DataStream s;
        s << "<fin "
          << "decision=" << std::to_string(decision) << " "
          << "cmd_idx=" << std::to_string(cmd_idx) << " "
          << "cmd_height=" << std::to_string(cmd_height) << " "
          << "cmd=" << get_hex10(cmd_hash) << " "
          << "blk=" << get_hex10(blk_hash) << ">";
        return s;
    }
};

}

#endif
