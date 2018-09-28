#!/usr/bin/python

# This is a dummy peer that just illustrates the available information your peers 
# have available.

# You'll want to copy this file to AgentNameXXX.py for various versions of XXX,
# probably get rid of the silly logging messages, and then add more logic.

import random
import logging

from messages import Upload, Request
from util import even_split, argmax
from peer import Peer
from collections import Counter

class DvStd(Peer):
    def post_init(self):
        print "post_init(): %s here!" % self.id
        self.n_unchoke_slots = 4
        self.dummy_state = dict()
        self.optimistic_set = []
        #self.dummy_state["cake"] = "lie"
    
    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see
        returns: a list of Request() objects
        This will be called after update_pieces() with the most recent state.
        """
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = filter(needed, range(len(self.pieces)))
        np_set = set(needed_pieces)  # sets support fast intersection ops.


        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_pieces))

        logging.debug("%s still here. Here are some peers:" % self.id)
        for p in peers:
            logging.debug("id: %s, available pieces: %s" % (p.id, p.available_pieces))

        logging.debug("And look, I have my entire history available too:")
        logging.debug(str(history))

        requests = []   # We'll put all the things we want here
        # Symmetry breaking is good...
        random.shuffle(needed_pieces)

        # Explain why
        random.shuffle(peers)
        

        needed_pieces_pop = [0 for _ in range(len(needed_pieces))]
        needed_pieces_peer = [[] for _ in range(len(needed_pieces))]

        if len(needed_pieces) > 0:
            for p in peers:
                for piece_id in p.available_pieces:
                    if piece_id in needed_pieces:
                        ind = needed_pieces.index(piece_id)
                        needed_pieces_pop[ind] += 1
                        needed_pieces_peer[ind].append(p)

            pop_sort_index = sorted(range(len(needed_pieces_pop)), key=needed_pieces_pop.__getitem__)
            needed_pieces = [needed_pieces[i] for i in pop_sort_index]
            needed_pieces_peer = [needed_pieces_peer[i] for i in pop_sort_index]

        num_request_to_peer = [0 for _ in range(len(peers))]
        for piece_id, piece_peers in zip(needed_pieces, needed_pieces_peer):
            peer_available = [piece_peers.index(p) for p in piece_peers if num_request_to_peer[peers.index(p)] < self.max_requests]
            if len(peer_available) > 0:
                peer = random.choice([piece_peers[i] for i in peer_available])
                start_block = self.pieces[piece_id]
                r = Request(self.id, peer.id, piece_id, start_block)
                requests.append(r)
                num_request_to_peer[peers.index(peer)] += 1

        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        round_num = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round_num))
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.
        
        SLOT_NUMBER = 4
        OPTOMISTIC_SLOT_NUMBER = 1

        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        elif round_num == 0:
            logging.debug("Still here: uploading to a random peer")
            # change my internal state for no reason
            self.dummy_state["cake"] = "pie"

            request = random.choice(requests)
            chosen = [request.requester_id]
            # Evenly "split" my upload bandwidth among the one chosen requester
            bws = even_split(self.up_bw, len(chosen))

        else:
            # Look at the prior round and determine how much we give
            requester_ids = [request.requester_id for request in requests]
            #Now grab information about their upload to you
            prior_downloads = []
            for i in range(0, 3):    
                prior_downloads += history.downloads[history.last_round() - i]
            
            prior_senders_ids = [d.from_id for d in prior_downloads]
            logging.debug("Prior downloands is " + str(prior_downloads))

            prior_per_id = Counter()
            for elem in prior_downloads:
                prior_per_id[elem.from_id] += elem.blocks
            requester_ids = set(requester_ids)

            # Number of unique users/slots we'll use 
            n = min(len(set(requester_ids)), SLOT_NUMBER)
            logging.debug("Prior per id is " + str(prior_per_id))
           # print("N is %d and max up bw is %d" % (n, self.up_bw) )
            
            # Here we want to calculate how many slots we'll use to give to
            # users who've given us bandwidth. We cap it at SLOT NUMBER - OPTOMOISTC 
            # as we want 1 optimisitc user. If the rquest length is smaller than we 
            # by defintion give it to everyone. otherwise we give to 
            # those who gave us stuff and who want stuff. The rest will go optomistically
            downloaders_requesters = set(requester_ids).intersection(set(prior_senders_ids))
            m = min(SLOT_NUMBER - OPTOMISTIC_SLOT_NUMBER, len(requester_ids), len(downloaders_requesters))
            # Now take the counter and remove the intersection 
            f1 = lambda x : x in downloaders_requesters
            counter_filtered = Counter(filter(f1, prior_per_id))
#            prior_downloads = sorted(prior_downloads, key = lambda x : x.blocks, reverse = True)
              
            # get the users who give us the most bandwidth in prior round
            top_m = counter_filtered.most_common(m)
            chosen = []
            bws = []
            if(len(top_m)):
                chosen = [d[0] for d in top_m]
                bws = [(self.up_bw/n) for d in xrange(len(top_m))]
           # Now who'se left figure out how much to just choose randomly
            
            if(len(self.optimistic_set ) == 0 or round_num % 3 == 0):  
                logging.debug("Checking random")
                remaining_candidates = set(requester_ids) - set(chosen)
                logging.debug("Candidates are" + str(remaining_candidates))
                v = n - len(chosen)
                logging.debug("v is %d" % v)
                remaining = random.sample(remaining_candidates, n - len(chosen))
                self.optimistic_set = remaining
                bws += [(self.up_bw/n) for d in xrange(len(remaining))]
                chosen += remaining
            elif( len(self.optimistic_set)):
                remaining = self.optimistic_set
                bws += [(self.up_bw/n) for d in xrange(len(remaining))]
                chosen += remaining
            logging.debug("Optomistic is " + str(self.optimistic_set))
            if(sum(bws) != self.up_bw): #Deal with rounding
                to_even_split = even_split(self.up_bw - sum(bws), len(bws))
                bws = [sum(x) for x in zip(bws, to_even_split)]
            

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]
            
        return uploads
