function(doc) {
	function handleChildren(node, parents){
		var parentDescriptor = node.id || node.title;
		for (var i in parents){
			emit(parents[i], parentDescriptor)
		}
		if(node.children){
			for(var i in node.children){
				handleChildren(node.children[i], parents.concat([parentDescriptor]))
			}
		}
	}
 	handleChildren(doc, []);
}