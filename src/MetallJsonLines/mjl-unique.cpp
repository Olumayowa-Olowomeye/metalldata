// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements head function to return N entries from a MetallJsoneLines

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>
//~ #include <ranges>

#include <boost/json.hpp>

#include "mjl-common.hpp"
#include "clippy/clippy-eval.hpp"
#include <set>
#include <ygm/container/set.hpp>
namespace xpr     = experimental;
namespace bjsn = boost::json;

namespace
{

const std::string    methodName      = "unique";
const std::string    LIST    = "list"; //if true return list of unique elements
const std::string    COLUMNS         = "columns";
const ColumnSelector DEFAULT_COLUMNS = {};

} // anonymous
int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Returns the number of unique elements within the given columns"};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_optional<bool>(LIST, "Return the list of unique elements if true", false);
  clip.add_optional<ColumnSelector>(COLUMNS, "projection list (list of columns to put out)", DEFAULT_COLUMNS);
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");
  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::string    dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION); 
    xpr::MetallJsonLines lines{world, metall::open_read_only, dataLocation, MPI_COMM_WORLD};
    lines.filter(filter(world.rank(), clip));
                                             
    auto len = lines.count();
    std::vector<std::pair<std::string,int>> res;
    auto cols = clip.get<ColumnSelector>(COLUMNS);
    auto data = lines.head(1,projector(COLUMNS,clip));
    std::vector<std::string> type;
    for (auto j : cols){
      auto t = data.at(0).as_object().at(j); //change to case switch?
      if(t.is_int64()){
        ygm::container::set<int> uni(world);                           
        lines.forAllSelected([&j,&len,&uni](std::size_t x, auto& el){
          auto y= el.as_object().at(j);
          assert(y.is_int64());
          uni.async_insert(y.as_int64());
        });
        res.emplace_back(std::make_pair(j.append("_countUnique"),uni.size()));
      }else if(t.is_uint64()){
        ygm::container::set<int> uni(world);                           
        lines.forAllSelected([&j,&len,&uni](std::size_t x, auto& el){
          auto y= el.as_object().at(j);
          assert(y.is_uint64());
          uni.async_insert(y.as_uint64());
        });
        res.emplace_back(std::make_pair(j.append("_countUnique"),uni.size()));
      }else if(t.is_double()){
        ygm::container::set<int> uni(world);                           
        lines.forAllSelected([&j,&len,&uni](std::size_t x, auto& el){
          auto y= el.as_object().at(j);
          assert(y.is_double());
          uni.async_insert(y.as_double());
        });
        res.emplace_back(std::make_pair(j.append("_countUnique"),uni.size()));
      }else if(t.is_string()){
        ygm::container::set<std::string> uni(world);                           
        lines.forAllSelected([&j,&len,&uni](std::size_t x, auto& el){
          auto y= el.as_object().at(j);
          assert(y.is_string());
          uni.async_insert(y.as_string());
        });
        res.emplace_back(std::make_pair(j.append("_countUnique"),uni.size()));
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

