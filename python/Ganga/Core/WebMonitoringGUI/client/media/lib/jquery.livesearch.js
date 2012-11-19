(function($) {  
	var self = null;
 	
	$.fn.liveUpdate = function(list) {	
		return this.each(function() {
			new $.liveUpdate(this, list);
		});
	};
	
	$.liveUpdate = function (e, list) {
		this.field = $(e);
		this.list  = $('#' + list);
		if (this.list.length > 0) {
			this.init();
		}
	};
	
	$.liveUpdate.prototype = {
		init: function() {
			var self = this;
			this.setupCache();
			this.field.parents('form').submit(function() { return false; });
			this.field.keyup(function() { self.filter(); });
			self.filter();
		},
		
		filter: function() {
			if ($.trim(this.field.val()) == '') { this.list.children('li').show(); return; }
			this.displayResults(this.getScores(this.field.val().toLowerCase()));
		},
		
		setupCache: function() {
			var self = this;
			this.cache = [];
			this.rows = [];
			this.list.children('li').each(function() {
				self.cache.push(this.innerHTML.toLowerCase());
				self.rows.push($(this));
			});
			this.cache_length = this.cache.length;
		},
		
		displayResults: function(scores) {
			var self = this;
			this.list.children('li').hide();
			$.each(scores, function(i, score) { self.rows[score[1]].show(); });
		},
		
		getScores: function(term) {
			var scores = [];
			for (var i=0; i < this.cache_length; i++) {
				var score = this.cache[i].score(term);
				if (score > 0) { scores.push([score, i]); }
			}
			return scores.sort(function(a, b) { return b[0] - a[0]; });
		}
	}
})(jQuery);
