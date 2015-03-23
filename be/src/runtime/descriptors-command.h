/*
 * descriptors-command.h
 *
 *  Created on: Feb 26, 2015
 *      Author: elenav
 */

#ifndef SRC_RUNTIME_DESCRIPTORS_COMMAND_H_
#define SRC_RUNTIME_DESCRIPTORS_COMMAND_H_

#include <map>
#include <vector>

#include "runtime/hdfs-fs-cache.h"
#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

/** Command descriptor, thrift to c++ transition */
class CommandDescriptor {
 public:
	CommandDescriptor(const TRemoteShortCommand& cdesc) :
		display_name_(cdesc.display_name){
		type_ = cdesc.type;
	}

  virtual ~CommandDescriptor() {}

  /** executes command */
  virtual bool run() = 0;

  /**< validates command */
  virtual bool validate(const TRemoteShortCommand& cdesc) = 0;

  const std::string& name() const { return display_name_; }
  TRemoteShortCommandType::type type() const { return type_; }

 protected:
  dfsFS                         dfs_connection_; /**< dfs connection */
  std::string                   display_name_;   /**< command display name */
  TRemoteShortCommandType::type type_;           /**< command type */
};

/** Rename command descriptor, thrift to c++ transition */
class RenameCmdDescriptor : public CommandDescriptor{
public:
	RenameCmdDescriptor(const TRemoteShortCommand& cdesc) :
		CommandDescriptor(cdesc), m_dfs_path(cdesc.dfs_path), m_rename_set(cdesc.rename_set){
	}

	virtual ~RenameCmdDescriptor() {}

	bool run();
	bool validate(const TRemoteShortCommand& cdesc);

private:
	std::string                        m_dfs_path;   /**< remote fs path to establish connection on */
	std::map<std::string, std::string> m_rename_set; /**< dataset to move */
};

/** Delete command descriptor, thrift to c++ transition */
class DeleteCmdDescriptor : public CommandDescriptor{
public:
	DeleteCmdDescriptor(const TRemoteShortCommand& cdesc) :
		CommandDescriptor(cdesc), m_dfs_path(cdesc.dfs_path), m_deletion_set(cdesc.delete_set){
	}

	virtual ~DeleteCmdDescriptor() {}

	bool run();
	bool validate(const TRemoteShortCommand& cdesc);

private:
	std::string m_dfs_path;                  /**< remote fs path to establish connection on */
	std::vector<std::string> m_deletion_set; /**< dataset to delete */
};

}



#endif /* SRC_RUNTIME_DESCRIPTORS_COMMAND_H_ */
