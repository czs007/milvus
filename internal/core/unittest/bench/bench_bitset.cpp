// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstdint>
#include <string>
#include <iostream>
#include <chrono>
#include <functional>
#include <map>
#include <iomanip>
#include <random>
#include <cmath>

#include <boost_ext/dynamic_bitset_ext.hpp>
#include "common/Types.h"
#include "test_utils/Timer.h"

//using namespace milvus;
using MapType = std::map<std::string, std::function<double(int)>>;

//using ConcurrentBitset = milvus::ConcurrentBitset2;
using ConcurrentBitset = milvus::ConcurrentBitset;
using BitsetType = milvus::BitsetType;
using BitsetView = milvus::BitsetView;

constexpr int MODE_CURRENT_BITSET = 1;
constexpr int MODE_BOOST_BITSET = 2;

constexpr int N_BITS = 1024* 1024;
//constexpr int N_BITS = 128;
constexpr int N = N_BITS / 8;

const uint8_t BITS_SRC[8] = {
	0b11011001, 
	0b10010001, 
	0b01011011, 
	0b10011101, 
	0b10011000, 
	0b11011000, 
	0b10010011, 
	0b10011001, 
};


std::vector<uint8_t> DatasetL;
std::vector<uint8_t> DatasetR;
std::vector<float> RandomData;

std::vector<int64_t> RandomPos;

void boost_test(std::string func_name, int round);
void concurrent_test(std::string func_name, int round);
std::string view_to_string(BitsetView & view);

double test_dummy_func(int round);

double test_boost_dynamic_bitset_clear(int round);
double test_boost_dynamic_bitset_set(int round);
double test_boost_dynamic_bitset_or(int round);
double test_boost_dynamic_bitset_and(int round);
double test_boost_dynamic_bitset_flip(int round);
double test_boost_dynamic_bitset_or_assign(int round);
double test_boost_dynamic_bitset_and_assign(int round);


double test_concurrent_bitset_clear(int round);
double test_concurrent_bitset_set(int round);
double test_concurrent_bitset_or(int round);
double test_concurrent_bitset_or_assign(int round);
double test_concurrent_bitset_and(int round);
double test_concurrent_bitset_and_assign(int round);
double test_concurrent_bitset_flip(int round);

void gen_random_data() {
    RandomData.resize(N);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(1.0, 100.0);
    for (int i = 0; i < N; ++i) {
        RandomData[i] = dis(gen);
    }
}

void simulate_flush_data(std::vector<float>& random_data) {
    float sum = 0.0;
    for (int i = 0; i < random_data.size(); i++) {
        sum += random_data[i] + random_data[random_data.size() - 1 -i];
    }
}


void prepare_dataset(){
	DatasetL.resize(N);
	DatasetR.resize(N);
	RandomPos.resize(N);

	// Seed with a real random value, if available
	std::random_device rd;
    	// Choose a random mean between 1 and 6
    	std::mt19937 gen(rd());
    	std::uniform_int_distribution<int> uniform_dist(0, 7);
    	std::uniform_int_distribution<int> pos_uniform_dist(0, N);

	for (int i =0;i < N; i++) {
		DatasetL[i] = BITS_SRC[uniform_dist(gen)];
		DatasetR[i] = BITS_SRC[uniform_dist(gen)];
		RandomPos[i] = pos_uniform_dist(gen);
	}

}

double test_concurrent_bitset_and_assign(int round){
	auto l = ConcurrentBitset(N_BITS, DatasetL.data());
	auto r = ConcurrentBitset(N_BITS, DatasetR.data());
    	Timer timer;

	for (int i =0; i < round; i++){
		l &= r;
	}	
	return timer.get_overall_seconds();

}

double test_concurrent_bitset_clear(int round){
	auto l = ConcurrentBitset(N_BITS, DatasetL.data());
    	Timer timer;

	for (int i =0; i < round; i++){
		for (int j =0; j<N; j++){
		  l.clear(ConcurrentBitset::id_type_t(j));
		}
	}	
	return timer.get_overall_seconds();
}

double test_concurrent_bitset_set(int round){
	auto l = ConcurrentBitset(N_BITS, DatasetL.data());
    	Timer timer;

	for (int i =0; i < round; i++){
		for (int j =0; j<N_BITS; j++){
		  l.set(ConcurrentBitset::id_type_t(j));
		}
	}	
	return timer.get_overall_seconds();
}

double test_concurrent_bitset_or(int round){
	auto l = ConcurrentBitset(N_BITS, DatasetL.data());
	auto r = ConcurrentBitset(N_BITS, DatasetR.data());
    	Timer timer;
	for (int i =0; i < round; i++){
		//std::cout<<" round i:" << i<< std::endl;
		auto x = l|r;
	}	
	return timer.get_overall_seconds();
}

double test_concurrent_bitset_and(int round){
	auto l = ConcurrentBitset(N_BITS, DatasetL.data());
	auto r = ConcurrentBitset(N_BITS, DatasetR.data());
    	Timer timer;

	for (int i =0; i < round; i++){
		l & r;
	}	

	return timer.get_overall_seconds();
}

double test_concurrent_bitset_flip(int round){
	auto l = ConcurrentBitset(N_BITS, DatasetL.data());
    	Timer timer;

	for (int i =0; i < round; i++){
		l.negate();
	}	

	return timer.get_overall_seconds();
}

double test_concurrent_bitset_or_assign(int round) {

  	auto l = ConcurrentBitset(N_BITS, DatasetL.data());
        auto r_c_bitset = ConcurrentBitset(N_BITS, DatasetR.data());

    	Timer timer;
	for (int i =0; i < round; i++){
		l |= r_c_bitset;
	}	

	return timer.get_overall_seconds();
}

double test_boost_dynamic_bitset_clear(int round){
	auto viewL = BitsetView(DatasetL.data(), N_BITS);
  	auto x = view_to_string(viewL);
  	auto l = BitsetType(x);

    	Timer timer;
	for (int i =0; i < round; i++){
		for (int j =0; j<N_BITS; j++){
		  l.reset(j);
		}
	}	

	return timer.get_overall_seconds();
}

double test_boost_dynamic_bitset_set(int round){
	auto viewL = BitsetView(DatasetL.data(), N_BITS);
  	auto x = view_to_string(viewL);
  	auto l = BitsetType(x);

    	Timer timer;
	for (int i =0; i < round; i++){
		for (int j =0; j<N_BITS; j++){
		  l.set(j);
		}
	}
	return timer.get_overall_seconds();
}

double test_boost_dynamic_bitset_or(int round){
	auto viewL = BitsetView(DatasetL.data(), N_BITS);
  	auto x = view_to_string(viewL);
  	auto l = BitsetType(x);

	auto viewR = BitsetView(DatasetR.data(), N_BITS);
 	auto y = view_to_string(viewR);
  	auto r = BitsetType(y);


    	Timer timer;

	for (int i =0; i < round; i++){
		l|r;
	}
	return timer.get_overall_seconds();
}

double test_boost_dynamic_bitset_and(int round){
	auto viewL = BitsetView(DatasetL.data(), N_BITS);
  	auto x = view_to_string(viewL);
  	auto l = BitsetType(x);

	auto viewR = BitsetView(DatasetR.data(), N_BITS);
 	auto y = view_to_string(viewR);
  	auto r = BitsetType(y);


    	Timer timer;

	for (int i =0; i < round; i++){
		l&r;
	}

	return timer.get_overall_seconds();
}

double test_boost_dynamic_bitset_and_assign(int round){
	auto viewL = BitsetView(DatasetR.data(), N_BITS);
 	 auto x = view_to_string(viewL);
  	auto l = BitsetType(x);

	auto viewR = BitsetView(DatasetR.data(), N_BITS);
 	 auto y = view_to_string(viewR);
  	auto r = BitsetType(y);

	Timer timer;
	for (int i =0; i < round; i++){
		l&= r;
	}	

	return timer.get_overall_seconds();
}

double test_boost_dynamic_bitset_flip(int round){
	auto viewL = BitsetView(DatasetL.data(), N_BITS);
  	auto x = view_to_string(viewL);
  	auto l = BitsetType(x);

    	Timer timer;
	for (int i =0; i < round; i++){
		l.flip();
	}	
	return timer.get_overall_seconds();
}

double test_boost_dynamic_bitset_or_assign(int round){
	auto viewL = BitsetView(DatasetR.data(), N_BITS);
 	 auto x = view_to_string(viewL);
  	auto l = BitsetType(x);

	auto viewR = BitsetView(DatasetR.data(), N_BITS);
 	 auto y = view_to_string(viewR);
  	auto r = BitsetType(y);

    	Timer timer;
	for (int i =0; i < round; i++){
		l|= r;
	}	
	return timer.get_overall_seconds();
}


std::string view_to_string(BitsetView & view){

    const char one = '1';
    const char zero = '0';
    const size_t len = view.size();
    std::string s;
    s.assign(len, zero);

    for (size_t i = 0; i < len; ++i) {
        if (view.test(i)){
		s[len-i-1] = one;
	}
    }
    return s;
}





double test_dummy_func(int round){
	Timer timer;
	for (int i =0; i < round; i++){
		std::cout<<"Run default dummy function!" << std::endl;
	}	
	return timer.get_overall_seconds();
}



void _print_helper(int mode, std::string func_name, bool is_end){
	std::string title = "";
	if (mode == MODE_CURRENT_BITSET) {
		title = "concurrent bitset";
	}else if (mode == MODE_BOOST_BITSET){
		title = "boost dynamic bitset";
	}

	std::string done = "";
	if (is_end) {
		done = "done";	
	}
	std::string prefix = "start to test";
	if (is_end){
		prefix = "test ";
	}
	
        std::cout<< prefix << " " << title << " " << func_name << " " << done << std::endl;
}

void print_start(int mode, std::string func_name){
	_print_helper(mode, func_name, false);
}

void print_end(int mode, std::string func_name, double secs) {
	_print_helper(mode, func_name, true);
  	std::cout<< func_name << "cost:" << secs << " secs" << std::endl;
  	std::cout<< func_name << "======================="<< std::endl;
}


MapType BoostFuncMap = {
	{ "clear", test_boost_dynamic_bitset_clear},
	{ "set", test_boost_dynamic_bitset_set},
	{ "|", test_boost_dynamic_bitset_or},
	{ "|=", test_boost_dynamic_bitset_or_assign},
	{ "&", test_boost_dynamic_bitset_and},
	{ "&=", test_boost_dynamic_bitset_and_assign},
	{ "flip", test_boost_dynamic_bitset_flip},
};

MapType ConcurrentFuncMap = {
	{ "clear", test_concurrent_bitset_clear},
	{ "set", test_concurrent_bitset_set},
	{ "|", test_concurrent_bitset_or},
	{ "|=", test_concurrent_bitset_or_assign},
	{ "&", test_concurrent_bitset_and},
	{ "&=", test_concurrent_bitset_and_assign},
	{ "flip",test_concurrent_bitset_flip},
};

void boost_test(std::string func_name, int round){
	auto it = BoostFuncMap.find(func_name);
	if (it != BoostFuncMap.end()) {
		 auto func = it->second;
  		 print_start(MODE_BOOST_BITSET, func_name);
		 auto secs =  func(round);
  		 print_end(MODE_BOOST_BITSET, func_name, secs);
    	}else{
		std::cout<<"Not match any function!" << std::endl;
	}
}


void concurrent_test(std::string func_name, int round){
	auto it = ConcurrentFuncMap.find(func_name);
	 if (it != ConcurrentFuncMap.end()) {
		 auto func = it->second;
  		 print_start(MODE_CURRENT_BITSET, func_name);
		 auto secs =  func(round);
  		 print_end(MODE_CURRENT_BITSET, func_name, secs);
    	}else{
		std::cout<<"Not match any function!" << std::endl;
	}
}

void clear_cache() {
  simulate_flush_data(RandomData);
}


double test_boost_resize(bool value, int round){
	Timer timer;
	for (int i =0; i < round; i++){
		auto l = BitsetType{};
		l.resize(N_BITS, value);
	}
	return timer.get_overall_seconds();
}

int main() {
  std::string func_name = "";
  int round = 10000;

  gen_random_data();
  prepare_dataset();
  std::cout<<"GenerateDataset done"<<std::endl;

/*  
  func_name = "flip";
  boost_test(func_name, round);
  concurrent_test(func_name, round);

  clear_cache();
  func_name = "|=";
  boost_test(func_name, round);
  concurrent_test(func_name, round);

  clear_cache();
  func_name = "|";
  boost_test(func_name, round);
  concurrent_test(func_name, round);
  */

  /*
  clear_cache();
  func_name = "clear";
  boost_test(func_name, round);
  concurrent_test(func_name, round);
  */

  clear_cache();
  func_name = "set";
  boost_test(func_name, round);
  concurrent_test(func_name, round);

  /*
  clear_cache();
  func_name = "&";
  boost_test(func_name, round);
  concurrent_test(func_name, round);

  clear_cache();
  func_name = "&=";
  boost_test(func_name, round);
  concurrent_test(func_name, round);
  */
  

//  std::cout <<" resit to true, cost:" << test_boost_resize(true, round) << std::endl;
//  std::cout <<" resit to false, cost:" << test_boost_resize(false, round) << std::endl;

  return 0;
}
