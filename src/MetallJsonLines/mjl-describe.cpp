// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements head function to return N entries from a MetallJsoneLines

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>
#include <algorithm>
//~ #include <ranges>

#include <boost/json.hpp>

#include "mjl-common.hpp"
#include "clippy/clippy-eval.hpp"
#include <set>
#include <map>
#include <ygm/container/counting_set.hpp>
namespace xpr     = experimental;
namespace bjsn = boost::json;

namespace xpr     = experimental;
namespace bjsn = boost::json;

namespace
{

const std::string    methodName      = "describe";
const std::string    COLUMNS         = "columns";
const std::string    COUNT           = "num";

const ColumnSelector DEFAULT_COLUMNS = {};
}

template<typename K, typename V>
std::multimap<V, K> invertMap(std::map<K, V> const &map)
{
    std::multimap<V, K> multimap;
 
    for (auto const &pair: map) {
        multimap.insert(std::make_pair(pair.second, pair.first));
    }
 
    return multimap;
}

 // anonymous
int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Returns brief decription of the mjl"};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");
  clip.add_optional<ColumnSelector>(COLUMNS, "projection list (list of columns to put out)", DEFAULT_COLUMNS);
  clip.add_optional<int>(COUNT, "Number of elements to return for frequency table (string frame only)", 10);
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::string    dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    const std::size_t    ct      = clip.get<int>(COUNT);
    auto cols = clip.get<ColumnSelector>(COLUMNS);
    xpr::MetallJsonLines lines{world, metall::open_read_only, dataLocation, MPI_COMM_WORLD};

    lines.filter(filter(world.rank(), clip));
    
    auto len = lines.count();
    std::vector<std::pair<std::string,int>> res;
    
    auto data = lines.head(1,projector(COLUMNS,clip)); //Assumption is that entire column has same type


    for (auto j : cols){
      auto t = data.at(0).as_object().at(j);
      if(t.is_int64()){
        long int amax = INT64_MIN;
        long int amin= INT64_MAX;
        long int sum=0;                           
        lines.forAllSelected([&j,&res,&amax,&amin,&sum](std::size_t x, auto& el){
          auto temp = el.as_object().at(j);
          assert(temp.is_int64());
          auto y = temp.as_int64();
          amax = std::max(y,amax);
          amin = std::min(y,amin);
          sum+=y;
        });
        res.emplace_back(std::make_pair(j+"_max",amax));
        res.emplace_back(std::make_pair(j+"_min",amin));
        res.emplace_back(std::make_pair(j+"_avg",sum/len));
      }else if(t.is_uint64()){
        unsigned long int amax = 0;
        unsigned long int amin= UINT64_MAX;
        unsigned long int sum=0;                           
        lines.forAllSelected([&j,&res,&amax,&amin,&sum](std::size_t x, auto& el){
          auto temp = el.as_object().at(j);
          assert(temp.is_uint64());
          auto y = temp.as_uint64();
          amax = std::max(y,amax);
          amin = std::min(y,amin);
          sum+=y;
        });
        res.emplace_back(std::make_pair(j+"_max",amax));
        res.emplace_back(std::make_pair(j+"_min",amin));
        res.emplace_back(std::make_pair(j+"_avg",sum/len));
      }else if(t.is_double()){
        double amax = std::numeric_limits<double>::min();
        double amin = std::numeric_limits<double>::max();
        double sum=0;                           
        lines.forAllSelected([&j,&res,&amax,&amin,&sum](std::size_t x, auto& el){
          auto temp = el.as_object().at(j);
          assert(temp.is_double());
          auto y = temp.as_double();
          amax = std::max(y,amax);
          amin = std::min(y,amin);
          sum+=y;
        });
        res.emplace_back(std::make_pair(j+"_max",amax));
        res.emplace_back(std::make_pair(j+"_min",amin));
        res.emplace_back(std::make_pair(j+"_avg",sum/len));
      } 
      else if(t.is_string()){
        ygm::container::counting_set<std::string> freq(world);
        lines.forAllSelected([&j,&res,&freq,&world](std::size_t x, auto& el){
          auto temp = el.as_object().at(j).as_string();
          freq.async_insert(temp);
        });
        auto t = freq.topk(ct, [](const auto &a, const auto &b) { return a.second > b.second; });
        std::for_each(t.begin(), t.end(), [&world,&res](const auto &p) {
          res.emplace_back(std::make_pair(p.first,p.second));
        });
      }
    }
    if (world.rank() == 0) clip.to_return(std::move(res));
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}



