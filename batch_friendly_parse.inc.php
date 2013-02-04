<?php

/*

I used this parser with the SDBSelectIterator to clone data between SimpleDB domains for testing.

NOTE: There is a known bug if your data exceeds the size limit before the row limit.  (E.g., if 25 rows can ever weigh more than 1 Mb, you will get a fatal error from batch_put_attributes)

$rows_cloned = 0;
$batches_cloned = 0;
$old_domain_data = new SDBSelectIterator("SELECT * FROM `$old_domain`", "batch_friendly_sdb_parse");
if (!$old_domain_data->isOK()){
	error_out_and_die("Problem getting old domain data: " . $old_domain_data->error_message());
}
$rows = array();
foreach($old_domain_data as $row){
	$rows_cloned += 1;
	$rows = array_merge($rows, $row);
	if(count($rows) >= 25 or !$old_domain_data->next_valid()){
		$overwrite_stats = $sdb->batch_put_attributes($new_domain, $rows);
		if(!$overwrite_stats->isOK()){
			die("Failed to batch put: " . ((String)$overwrite_stats->body->Errors[0]->Error->Message) );
		}
		$batches_cloned += 1;
		$rows = array();
	}
}
echo "Success!  Cloned $rows_cloned rows in $batches_cloned batches.<br>";

*/


function batch_friendly_sdb_parse($item){
	$return = array();
	
	// Loop through the item's attributes
	foreach ($item->Attribute as $attribute){
		$column_name = (string) $attribute->Name;
		if($return[$column_name] and !is_array($return[$column_name])){
			//2nd attribute with same col name, make it an array
			$return[$column_name] = array(
				$return[$column_name],
				(string) $attribute->Value
			);
		}elseif($return[$column_name] and is_array($return[$column_name])){
			//Nth entry into existing array
			$return[$column_name][] = (string) $attribute->Value;
		}else{
			//1st entry
			$return[$column_name] = (string) $attribute->Value;
		}
	}	

	return array(
		(string) $item->Name => $return
	);//Calling code needs to array_merge these results together
}

?>
