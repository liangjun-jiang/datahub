package com.linkedin.metadata.kafka.spline;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


public class SplineRestController {
  @Autowired
  SplineRestService _splineRestService;

  public SplineRestController(SplineRestService splineRestService) {
    _splineRestService = splineRestService;
  }

  @RequestMapping(
      path = "/producer/status",
      method = RequestMethod.HEAD,
      produces = {"application/json"}
  )
  public ResponseEntity<String> producerStatus() throws Exception {
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @PostMapping(
      path = "/producer/execution-events",
      produces = {"application/json"}
  )
  public ResponseEntity<String> executeEvents(@RequestBody Map<String, Object> payload) {
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @PostMapping(
      path = "/producer/execution-plans",
      produces = {"application/json"}
  )
  public ResponseEntity<String> executePlans(@RequestBody Map<String, Object> payload) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new DefaultScalaModule());

    ExecutionPlan executionPlan = objectMapper.convertValue(payload, ExecutionPlan.class);
    _splineRestService.setExecutionPlan(executionPlan);
    _splineRestService.persistEntities();
    return new ResponseEntity<>(HttpStatus.OK);
  }
}
