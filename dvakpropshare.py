#!/usr/bin/python

# This is a dummy peer that just illustrates the available information your peers 
# have available.

# You'll want to copy this file to AgentNameXXX.py for various versions of XXX,
# probably get rid of the silly logging messages, and then add more logic.

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer

class DvAkPropshare(Peer):
    def post_init(self):
        print "post_init(): %s here!" % self.id
        self.optimistically_unchoked_peer = -1
        self.first_upload_round = -1
        #self.dummy_state = dict()
        #self.n_unchoke_slots = 4
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
        logging.debug("look at the AgentHistory class in history.py for details")
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
                av_set = set(p.available_pieces)
                isect = av_set.intersection(np_set)
                for piece_id in list(isect):
                        ind = needed_pieces.index(piece_id)
                        needed_pieces_pop[ind] += 1
                        needed_pieces_peer[ind].append(p)

            pop_sort_index = sorted(range(len(needed_pieces_pop)), key=needed_pieces_pop.__getitem__)
            needed_pieces = [needed_pieces[i] for i in pop_sort_index]
            needed_pieces_peer = [needed_pieces_peer[i] for i in pop_sort_index]

        num_request_to_peer = [0 for _ in range(len(peers))]

        for piece_id, piece_peers in zip(needed_pieces, needed_pieces_peer):
            peer_available = [piece_peers.index(p) for p in piece_peers if
                              num_request_to_peer[peers.index(p)] < self.max_requests]

            if len(peer_available) > 0:
                #for peer in [piece_peers[i] for i in peer_available]:
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
        round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round))
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.

        # Get list of unique requesters id
        requesters_id = list(set([request.requester_id for request in requests]))

        # get total download to self and download from each requester to self
        peers_share = [0 for _ in range(len(requesters_id))]
        total_dws = 0
        unchoke_peers = []
        if round > 1:
            for download in history.downloads[round - 1]:
                if (download.from_id in requesters_id) : #& (download.to_id == self.id): # count only dws to myself
                    from_peer = requesters_id.index(download.from_id)
                    bw = download.blocks
                    peers_share[from_peer] += bw
                    total_dws += bw

            if total_dws > 0:
                peers_share = [peers_share[i] / total_dws for i in range(len(peers_share))]

            # sort requesters and shares on their share in previous round downloads
            share_sort_index = sorted(range(len(peers_share)), key=peers_share.__getitem__, reverse=True)
            peers_share = [peers_share[i] for i in share_sort_index]
            requesters_id = [requesters_id[i] for i in share_sort_index]
            # select requesters who participated in self downloads
            unchoke_peers = [requesters_id[i] for i in range(len(peers_share)) if peers_share[i] > 0]

        chosen = []
        bws = []

        # Assign bandwidth to requesters

        if len(requests) > 0:
            if self.first_upload_round == -1:
                self.first_upload_round = round

            sum_bws = 0
            i=0
            while (sum_bws < 0.9 * self.up_bw) & (i < len(unchoke_peers)):
                chosen.extend([unchoke_peers[i]])
                bws.extend([0.9 * peers_share[i] * self.up_bw])
                sum_bws += 0.9 * peers_share[i] * self.up_bw
                i += 1

            #if ((round - self.first_upload_round) % 3 == 0) & (round > self.first_upload_round - 1):
            self.optimistically_unchoked_peer = random.sample(requesters_id, 1)

            # Optimistic unchoking for the rest of the bandwidth
            chosen.extend(self.optimistically_unchoked_peer)
            bws.extend([self.up_bw - sum_bws])

            print(chosen, bws)

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        return uploads
