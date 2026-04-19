Project Context: AI Supply Chain Strategist

This project is a multi-agent system designed for autonomous supply chain disruption management and strategic planning. It utilizes LangGraph for orchestration, Gemini 2.5 for multimodal vision and high-level reasoning, and Databricks for structured enterprise data.

🛠 Tech Stack

Orchestration: LangGraph (Stateful Multi-Agent Framework)

AI Models: Gemini 2.0 Flash (Fast Vision/Logic), Gemini 2.5 Pro (Advanced Strategy/Reasoning)

Data: Databricks SQL / PySpark (Mocked locally with SQLite for development)

Environment: Docker / Python 3.12+ / Databricks SDK

📋 Coding Standards

Style: Clean, maintainable, modular Python code following PEP 8.

Typing: Use strict type hints (typing.TypedDict, Annotated) for all Graph states to ensure runtime safety.

Documentation: Google-style docstrings. All agents must include a clear "Business Objective" in their class definition.

Error Handling: Implement exponential backoff for API failures (Gemini/Databricks). Graceful fallbacks for vision processing errors.

Output: Prefer structured JSON outputs (Pydantic models) for inter-agent communication.

🚀 Common Commands

Development

Initialize DB: python mock_data/init_db.py

Run Workflow: python main.py

Run Tests: pytest tests/

Environment

Install Deps: pip install -r requirements.txt

Docker Build: docker-compose build

Docker Up: docker-compose up -d

🧠 Strategic Frameworks (Business Alignment)

When generating "Executive Summaries" or "Strategic Mitigation Plans":

Analyze 3 Pillars: Short-term Mitigation, Mid-term Resilience, Long-term Sustainability.

Tone: Professional, objective, data-driven, and action-oriented.

Validation: Every recommendation must be grounded in data provided by the inventory_agent or specific visual observations from the supplier_agent.

📂 Key File Instructions

src/graph/state.py: Defines the AgentState including messages, inventory_data, risk_analysis, and vision_reports.

src/agents/: Each agent (Demand, Inventory, Supplier, Supervisor) must be a standalone LangGraph node.

src/databricks/client.py: Supports DATABRICKS_RUNTIME environment variable to toggle between cloud-native SQL and local SQLite mock data.