<?php

class SDBSelectIterator implements Iterator {
	private $position = 0;
	private $total_position = 0;
	private $next_token = null;
	private $result_batch = null;
	private $query = "";
	private $parser = false;
	private $sdb = null;
	private $ok = true;
	private $err = "";

	public function __construct($query, $parser = false) {
		if(!class_exists('AmazonSDB')){
			require_once('AWSSDKforPHP/sdk.class.php');
		}
		$this->position = 0;
		$this->total_position = 0;
		$this->query = $query;
		$this->next_token = null;
		$this->parser = $parser;
		$this->sdb = new AmazonSDB();
		$this->query();
	}

	public function isOK(){
		return $this->ok;
	}

	public function error_message(){
		return $this->err;

	}

	private function query(){
		if ($this->next_token){
			$response = $this->sdb->select($this->query, array(
				'NextToken' => $this->next_token,
			));
		}else{
			$response = $this->sdb->select($this->query);
		}	
		$this->position = 0;
		$this->next_token = isset($response->body->SelectResult->NextToken)
			? (string) $response->body->SelectResult->NextToken
			: null;
		
		if (!$response->isOK()){
			$this->ok = false;
			$this->err = "";
			if($response->body->Errors){
				foreach($response->body->Errors as $error){
					$this->err .= $error->Error->Message . " ";
				}
			}
			$this->err = trim($this->err);
		}else{
			$this->ok = true;
		}
		
		$this->result_batch = array();
		if($response->body->Item()){
			foreach($response->body->Item() as $item){
				$this->result_batch[] = $this->parse($item);
			}
		}
	}

	private function parse($row){
		if($this->parser and function_exists($this->parser)){
			$parser = $this->parser;
			return $parser($row);
		}else{
			return $this->generic_sdb_parse($row);
		}
	}

	private function generic_sdb_parse($item){
		$return = array(
			'primary_key' => (string) $item->Name
		);

		// Loop through the item's attributes
		foreach ($item->Attribute as $attribute){
			$column_name = (string) $attribute->Name;
			if(preg_match('/^\{.*\}$/',(string) $attribute->Value)){ //Data is plausibly JSON
				$parsed_json = json_decode((string) $attribute->Value, true);
				if($parsed_json === null){
					//Guess it wasn't JSON after all
					$return[$column_name] = (string) $attribute->Value;
				}else{
					$return[$column_name] = $parsed_json;
				}
			}elseif(preg_match('/^[\-+]?\d+(:?\.\d+)?$/', (string) $attribute->Value)){ //Numeric
				$return[$column_name] = floatval((string) $attribute->Value);
			}else{ //Treat as dumb string
				$return[$column_name] = (string) $attribute->Value;
			}
		}	

		return $return;
	}

	function rewind() {
		//var_dump(__METHOD__);
		if($this->total_position == 0){
			return;
		}
		$this->position = 0;
		$this->total_position = 0;
		$this->next_token = null;
		$this->query();
	}

	function current() {
		//var_dump(__METHOD__);
		return $this->result_batch[$this->position];
	}

	function key() {
		//var_dump(__METHOD__);
		return $this->total_position;
	}

	function next() {
		//var_dump(__METHOD__);
		$this->position += 1;
		$this->total_position += 1;
		if(!isset($this->result_batch[$this->position]) && $this->next_token){
			$this->query();
		}
	}

	function next_valid() {
		return (isset($this->result_batch[$this->position + 1]) or $this->next_token);
	}

	function valid() {
		//var_dump(__METHOD__);
		if(!$this->result_batch or !is_array($this->result_batch)){
			return false;
		}
		return isset($this->result_batch[$this->position]);
	}
}

?>
