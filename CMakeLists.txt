#==============================================================================

set(INSTALL_BINDIR ${CMAKE_SOURCE_DIR}/bin)
set(INSTALL_LIBDIR ${CMAKE_SOURCE_DIR}/lib)

#foreach(lvar IN ITEMS Sampler; Sink; NullDevice;
foreach(lvar IN ITEMS
	RoestiSampler;
	RoestiSink;
)
  set(EXEC ${lvar})
  add_executable(${EXEC} 
    ${EXEC}.cxx;
  )

  target_include_directories(${EXEC} PUBLIC 
    ${Boost_INCLUDE_DIRS};  
    ${FairMQ_INCDIR};
    ${FairMQ_INCDIR}/fairmq;
    #${FairMQ_INCDIR}/fairlogger/bundled;
    ${FairLogger_INCDIR};
    ${CMAKE_SOURCE_DIR};
    ${HIREDIS_HEADER};
    ${REDIS_PLUS_PLUS_HEADER};
    ./;
  )

  target_link_directories(${EXEC} PUBLIC
    ${Boost_LIBRARY_DIRS}; 
    ${FairMQ_LIBDIR};
    #${ROOT_LIBRARIES};
  )

  target_link_libraries(${EXEC} PUBLIC 
    ${Boost_LIBRARIES};  
    #${fmt_LIB};
    FairMQ;
    FairMQStateMachine;
    FairLogger;
    #${ROOT_LIBRARIES};
    #${HIREDIS_LIB};
    #${REDIS_PLUS_PLUS_LIB};
  )

  install(TARGETS
    ${EXEC};
    #LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    #RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${INSTALL_LIBDIR}
    RUNTIME DESTINATION ${INSTALL_BINDIR}
  )
endforeach()



