# Self-Healing Data Pipeline

A full-stack demo application showcasing an intelligent data quality pipeline with AI-powered learning capabilities.

## Features

- **Real-time Data Ingestion**: Submit data through an intuitive form interface
- **Automated Quality Detection**: Identifies dirty data using predefined rules
- **Multi-Stage Healing Process**:
  1. Local Rules Engine - Applies human-defined cleaning rules
  2. AI Healing - Uses simulated Gemini AI to handle complex cases
  3. Learning System - Automatically adds successful healing patterns to rulebook
- **Visual Pipeline Flow**: Watch data progress through each stage with live status updates
- **Side-by-Side Comparison**: See original vs cleaned data with highlighted changes
- **Smart Rulebook**: Tracks both human-defined and AI-generated rules
- **Complete Audit Trail**: Timeline view showing all transformations

## Tech Stack

### Frontend
- React 18 with TypeScript
- Tailwind CSS for styling
- Lucide React for icons
- Vite for build tooling

### Backend
- Node.js with Express
- In-memory JSON storage
- Simulated Gemini AI responses

## Getting Started

### Prerequisites
- Node.js (v18 or higher)
- npm or yarn

### Installation

1. Install dependencies:
```bash
npm install
```

### Running the Application

You need to run both the backend server and the frontend development server.

#### Terminal 1 - Backend Server:
```bash
npm run server
```
This starts the Express API on `http://localhost:3001`

#### Terminal 2 - Frontend Dev Server:
```bash
npm run dev
```
This starts the Vite dev server (typically on `http://localhost:5173`)

### Building for Production

```bash
npm run build
```

## Usage Guide

### Testing with Dirty Data

Click the "Load Dirty Data" button to populate the form with intentionally dirty data:
- Name with extra whitespace
- Email in mixed case
- Negative age
- Salary with currency symbols and text
- Empty country field

### Testing with Clean Data

Click the "Load Clean Data" button to see how the system handles already-clean data.

### Understanding the Pipeline Stages

1. **Ingestion** - Raw data is received and stored
2. **Detection** - System analyzes data quality and identifies issues
3. **Local Rules** - Applies existing rules from the rulebook
4. **AI Healing** - Simulated AI suggests fixes for remaining issues
5. **Learning** - New rules are added to the rulebook
6. **Output** - Final clean data is delivered

### Viewing Results

After processing, you'll see:
- **Pipeline Flow**: Visual representation of each stage with logs
- **Data Comparison**: Side-by-side view of original vs cleaned data
- **Rulebook**: Complete list of all rules (human and AI-generated)
- **Timeline**: Chronological audit trail of all transformations

## How Self-Healing Works

The system demonstrates a learning pipeline that:

1. **Detects** data quality issues using validation rules
2. **Decides** which healing strategy to apply (local rules vs AI)
3. **Learns** from AI suggestions by adding new rules to the rulebook
4. **Reuses** learned rules on future data, reducing AI calls

This creates a system that gets smarter over time, similar to production data quality platforms.

## API Endpoints

### POST `/api/ingest`
Processes incoming data through the self-healing pipeline.

**Request Body:**
```json
{
  "name": "string",
  "email": "string",
  "age": "string",
  "salary": "string",
  "country": "string"
}
```

**Response:**
```json
{
  "success": true,
  "original": {},
  "cleaned": {},
  "dirtyFields": [],
  "appliedRules": [],
  "isClean": true,
  "logs": [],
  "rulebook": []
}
```

### GET `/api/rulebook`
Returns the current rulebook.

### GET `/api/history`
Returns all processed data records.

### DELETE `/api/reset`
Clears all stored data.

## Architecture

### Backend Components

- **server/index.js** - Express server and main processing logic
- **server/detection.js** - Data quality detection rules
- **server/rules.js** - Local rules engine
- **server/ai-healing.js** - Simulated AI healing with Gemini

### Frontend Components

- **DataForm** - Input form with example data loaders
- **PipelineFlow** - Visual pipeline with stage indicators
- **PipelineStage** - Individual stage card with status and logs
- **ComparisonView** - Side-by-side data comparison
- **RulebookPanel** - Display of all rules with filtering
- **TimelineView** - Chronological audit trail

## Design Philosophy

The UI is designed to look like an enterprise data observability platform:
- Dark mode with glassmorphism effects
- Clear visual hierarchy
- Real-time status updates
- Professional color scheme (no purple/indigo)
- Smooth animations and transitions
- Responsive layout for all screen sizes

## Future Enhancements

- Connect to real Gemini API
- Add persistent storage (database)
- Support for batch data processing
- Rule editing and management UI
- Advanced analytics and reporting
- Export clean data functionality
- Custom validation rule builder

## License

This is a demo application for educational purposes.
