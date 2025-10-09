const FilterTemplate = require("../models/filterTemplate.model");

const createTemplate = async (data) => {
  const templateData = {
    name: data.name,
    description: data.description || "",
    isDefault: data.isDefault || false,
    createdBy: data.createdBy || "user",
  };

  // Handle new complete state format
  if (data.state) {
    templateData.state = {
      filters: {
        bought: data.state.filters?.bought || "all",
        newArrival: data.state.filters?.newArrival || "all",
        fulfillment: data.state.filters?.fulfillment || "all",
        fbaFee: data.state.filters?.fbaFee || "all",
        price: data.state.filters?.price || "all",
        referralFee: data.state.filters?.referralFee || "all",
        rating: data.state.filters?.rating || "all",
      },
      sellers: data.state.sellers || [],
      categories: data.state.categories || [],
      sources: data.state.sources || [],
      sort: data.state.sort || "",
      displayMode: data.state.displayMode || "grid",
      imageOnlyCols: data.state.imageOnlyCols || 0,
    };
    // Also populate legacy filters for backward compatibility
    templateData.filters = {
      bought: data.state.filters?.bought || "all",
      newArrival: data.state.filters?.newArrival || "all",
      fulfillment: data.state.filters?.fulfillment || "all",
      fbaFee: data.state.filters?.fbaFee || "all",
      price: data.state.filters?.price || "all",
      rating: data.state.filters?.rating || "all",
    };
  } else {
    // Legacy format - only filters
    templateData.filters = {
      bought: data.filters?.bought || "all",
      newArrival: data.filters?.newArrival || "all",
      fulfillment: data.filters?.fulfillment || "all",
      fbaFee: data.filters?.fbaFee || "all",
      price: data.filters?.price || "all",
      rating: data.filters?.rating || "all",
    };
  }

  return await FilterTemplate.create(templateData);
};

const getAllTemplates = async () => {
  return await FilterTemplate.find().sort({ isDefault: -1, name: 1 });
};

const getTemplateById = async (id) => {
  return await FilterTemplate.findById(id);
};

const getTemplateByName = async (name) => {
  return await FilterTemplate.findOne({ name });
};

const updateTemplate = async (id, data) => {
  const updateData = {
    name: data.name,
    description: data.description || "",
    isDefault: data.isDefault || false,
  };

  // Handle new complete state format
  if (data.state) {
    updateData.state = {
      filters: {
        bought: data.state.filters?.bought || "all",
        newArrival: data.state.filters?.newArrival || "all",
        fulfillment: data.state.filters?.fulfillment || "all",
        fbaFee: data.state.filters?.fbaFee || "all",
        price: data.state.filters?.price || "all",
        referralFee: data.state.filters?.referralFee || "all",
        rating: data.state.filters?.rating || "all",
      },
      sellers: data.state.sellers || [],
      categories: data.state.categories || [],
      sources: data.state.sources || [],
      sort: data.state.sort || "",
      displayMode: data.state.displayMode || "grid",
      imageOnlyCols: data.state.imageOnlyCols || 0,
    };
    // Also update legacy filters for backward compatibility
    updateData.filters = {
      bought: data.state.filters?.bought || "all",
      newArrival: data.state.filters?.newArrival || "all",
      fulfillment: data.state.filters?.fulfillment || "all",
      fbaFee: data.state.filters?.fbaFee || "all",
      price: data.state.filters?.price || "all",
      rating: data.state.filters?.rating || "all",
    };
  } else {
    // Legacy format - only filters
    updateData.filters = {
      bought: data.filters?.bought || "all",
      newArrival: data.filters?.newArrival || "all",
      fulfillment: data.filters?.fulfillment || "all",
      fbaFee: data.filters?.fbaFee || "all",
      price: data.filters?.price || "all",
      rating: data.filters?.rating || "all",
    };
  }

  return await FilterTemplate.findByIdAndUpdate(id, updateData, {
    new: true,
  });
};

const deleteTemplate = async (id) => {
  return await FilterTemplate.findByIdAndDelete(id);
};

const deleteAllTemplates = async () => {
  return await FilterTemplate.deleteMany();
};

module.exports = {
  createTemplate,
  getAllTemplates,
  getTemplateById,
  getTemplateByName,
  updateTemplate,
  deleteTemplate,
  deleteAllTemplates,
};
