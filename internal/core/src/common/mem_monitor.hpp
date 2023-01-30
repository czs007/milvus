// mem monitor
// Copyright (c) 2014, Matthias Petri, All rights reserved.

// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 3.0 of the License, or (at your option) any later version.

// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
#pragma once

#include <chrono>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <thread>
#include <atomic>
#include <stdexcept>

using std::vector;

struct mem_stat {
    uint64_t pid;
    uint64_t VmPeak;
    uint64_t VmSize;
    uint64_t VmHWM;
    uint64_t VmRSS;
    uint64_t VmData;
    uint64_t VmPTE;
    uint64_t event_id;
};

class mem_monitor
{
    private:

        uint64_t extract_number(std::string& line, size_t start_pos,
                                bool extension = false) {
            auto num_end = line.find_first_of(' ', start_pos);
            if (num_end == std::string::npos) {
                num_end = line.size() - 1;
            }
            uint64_t num = std::strtoull(line.c_str() + start_pos, NULL, 10);

            if (extension) {
                if (line.back() == 'B') {
                    if (line[line.size() - 2] == 'k' || line[line.size() - 2] == 'K') {
                        num *= 1024;
                    }
                    if (line[line.size() - 2] == 'm' || line[line.size() - 2] == 'M') {
                        num *= 1024 * 1024;
                    }
                    if (line[line.size() - 2] == 'g' || line[line.size() - 2] == 'G') {
                        num *= 1024 * 1024 * 1024;
                    }
                } else {
                    throw std::invalid_argument("no extension found during line parsing");
                }
            }
            return num;
        }

    public:

	std::string get_value_str() {
		auto s = get_current_stats();
            	std::stringstream ss;
	    	ss << s.VmPeak << ";"
			<< s.VmSize << ";"
		       << s.VmHWM  << ";"
		       << s.VmRSS  << ";"
		       << s.VmData << ";"
		       << s.VmPTE  << ";"
		       << "\n";
		std::string ret;
		ss >> ret;
		return ret;
	}

        //mem_monitor(const std::string& file_name,
        mem_monitor(){
            // some init stuff
        }

        ~mem_monitor() {
        }

        mem_stat get_current_stats() {
            mem_stat stat;

            // read memory stats
            {
                std::ifstream pfs("/proc/self/status");
                std::string line;
                while (std::getline(pfs, line)) {
                    auto key_end_pos = line.find(':');
                    auto value_start_pos = line.find_first_not_of('\t', key_end_pos + 1);
                    auto key = line.substr(0, key_end_pos);
                    if (key == "Pid") {
                        stat.pid = extract_number(line, value_start_pos);
                    }
                    if (key == "VmPeak") {
                        stat.VmPeak = extract_number(line, value_start_pos, true);
                    }
                    if (key == "VmSize") {
                        stat.VmSize = extract_number(line, value_start_pos, true);
                    }
                    if (key == "VmHWM") {
                        stat.VmHWM = extract_number(line, value_start_pos, true);
                    }
                    if (key == "VmRSS") {
                        stat.VmRSS = extract_number(line, value_start_pos, true);
                    }
                    if (key == "VmData") {
                        stat.VmData = extract_number(line, value_start_pos, true);
                    }
                    if (key == "VmPTE") {
                        stat.VmPTE = extract_number(line, value_start_pos, true);
                    }
                }
            }
            return stat;
        }
};
