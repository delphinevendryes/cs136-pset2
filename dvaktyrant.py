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
from collections import Counter
from copy import deepcopy


class DvAkTyrant(Peer):
    def post_init(self):
        print "post_init(): %s here!" % self.id
        self.dummy_state = dict()
        self.dummy_state["cake"] = "lie"
        self.tau_dict = dict() # Key is peer_id value is tau
        self.broadcast_history = dict() #Braodcast history key -> round # and value -> dict where key is peer id and value is array of pieces
        self.expected_download_rate = dict() # Download rate 
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
        
        # Sort peers by id.  This is probably not a useful sort, but other 
        # sorts might be useful
        peers.sort(key=lambda p: p.id)
        
        lst_elem = [ peer.available_pieces for peer in peers ]
        flat_list = [item for sublist in lst_elem for item in sublist]
        # Gives us the number of pieces we can get by peers. Now when
        # we go and request from reach pier we can sort by this list
        elem_counter = Counter(flat_list)
        
        # request all available pieces from all peers!
        # (up to self.max_requests from each)
        
        # We want to have a "raity" metric where we know everyone who's requesting
        
        
        for peer in peers:
            av_set = set(peer.available_pieces)
            isect = av_set.intersection(np_set)
            n = min(self.max_requests, len(isect))
            # More symmetry breaking -- ask for random pieces.
            # This would be the place to try fancier piece-requesting strategies
            # to avoid getting the same thing from multiple peers at a time.
            
            #Create a list needed to define what s1and s2 are
            lst_isec = list(isect)
            random.shuffle(lst_isec)
            lst_isec.sort(key= lambda piece: elem_counter[piece], reverse = False)    
            lst_isec = lst_isec[:n] 
            for piece_id in lst_isec:
                # aha! The peer has this piece! Request it.
                # which part of the piece do we need next?
                # (must get the next-needed blocks in order)
                start_block = self.pieces[piece_id]
                r = Request(self.id, peer.id, piece_id, start_block)
                requests.append(r)

        return requests



    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """
        
        
        requester_ids = [request.requester_id for request in requests]
        logging.debug("All pers are "+ str(peers))

        gamma = 0.1
        num_rounds = 3
        alpha = 0.2
        # Default tau list
        if(len(self.tau_dict) == 0 ):
            for peer in peers:
                self.tau_dict[peer.id] = 1./4. * self.up_bw

        #Compute the expected download speed by looking 
        #at what they broadcasted over a period of R times and 
        #computing the difference

        round = history.current_round()
        r1 = history.current_round()
        self.broadcast_history[r1] = deepcopy(peers)
        logging.debug("%s again.  It's round %d." % (
            self.id, round))
        
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.

        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        elif(round > 4):
            logging.debug("Trying to use a bittryant strategy")
            '''First get the history from the prior rounds to get a sense 
            of everyone else's download speed etc'''
            prior_downloads_sets = []
            prior_downloads = []
            
            random.shuffle(peers)

            
            period_lookback = min(len(history.downloads), num_rounds)
            for i in range(0, period_lookback ):    
                temp = history.downloads[history.last_round() - i]
                prior_ids = [x.from_id for x in temp]
                prior_downloads += history.downloads[history.last_round() - i]
                prior_downloads_sets.append(set(prior_ids))
    
            # Get the interesction to see who's given us a lot
            good_peers = set.union(*prior_downloads_sets)
            logging.debug("These peers have been good to me so lets try to reduce how much they give us " + str(good_peers))
            for pid in good_peers:
                self.tau_dict[pid] *=  (1 - gamma)

            logging.debug("These peers didn't give us anything last rounds let-s bumpt up tau " + str(set(peers) - set(prior_downloads_sets[0])) )
            for peer in peers:
                if(peer.id not in prior_downloads_sets[0]):
                    self.tau_dict[peer.id] = min( self.tau_dict[peer.id] *(1 + alpha), self.up_bw * 1.0)

            #Handle stuff from the prior period to adjust the tau/
            for download_info in history.downloads[history.last_round()]:
                self.expected_download_rate[download_info.from_id] = download_info.blocks
                            
            
           # Now process the current round of stuff
           # First we compare the broadcasted amoutn in this round 
           # vs several rounds ago to see how much has changed
            later_counter = Counter()
            for peer in peers:
                later_counter[peer.id] = len(peer.available_pieces)
            prior_counter = Counter()
            for p2 in self.broadcast_history[round - period_lookback]:
              prior_counter[p2.id] = len(p2.available_pieces)
            
            # This is delta of pieces to estimate how much capacity is there
            diff_counter = later_counter - prior_counter
            
            # This computes expected donwload capacity
            for elem in diff_counter:
                diff_counter[elem] *= 1./3. * self.conf.blocks_per_piece / 4

            # Add in what we actually know
            for peer in self.expected_download_rate:
                diff_counter[peer] = self.expected_download_rate[peer]

            #If peer is not in here then assign it a low value. Note if it's 
            # zero then our sort would just be completely random
            for peer in peers:
                if(peer.id not in diff_counter):
                    diff_counter[peer.id] = 0
            
            logging.debug("Our Fji is now " + str(diff_counter))
            #Need a deepcopy to keep track of the state
            ordering = deepcopy(diff_counter)
            
            # Here we go ahead and compute our ordering metric which is the 
            # expected download speed vs the expected upload amount required
            # to have recipcation 
            for elem in ordering: 
                ordering[elem] /= self.tau_dict[elem]
            ordering = ordering.most_common()
            

            # now it's sorted - go through the list and see the top 
            # many until we can fill out our capacity
            cap = self.up_bw
            total = 0
            chosen = []
            bws = []
            order_lst = list(ordering)
            i = 0
            while(i < len(ordering) and total <= cap):
                to_append = min(int(diff_counter[order_lst[i][0]]), self.conf.blocks_per_piece)
                if order_lst[i][0] in requester_ids and total + to_append <= cap:
                    chosen.append(order_lst[i][0])
                    bws.append(to_append)
                    total += to_append
                i+=1 
            
            remaining = self.up_bw - sum(bws)
            if(remaining):
                to_even_split = even_split(self.up_bw - sum(bws), len(bws))
                bws = [sum(x) for x in zip(bws, to_even_split)]

                                
        else:
            logging.debug("Still here: uploading to a random peer")
            # change my internal state for no reason
            self.dummy_state["cake"] = "pie"

            request = random.choice(requests)
            chosen = [request.requester_id]
            # Evenly "split" my upload bandwidth among the one chosen requester
            
            bws = even_split(self.up_bw, len(chosen))


        logging.debug("Uploads are to " + str(chosen ) )
        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]
            
        return uploads
